package server;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;
import static utils.CommunicationUtils.*;
import static utils.HashUtils.generateID;
import static utils.PlasmaUtils.*;

@Slf4j
public class InfinimumDBServer {


    private static final int OPERATION_MESSAGE_SIZE = 10;
    final transient int serverID = 0;
    transient int serverCount = 1;

    private transient PlasmaClient plasmaClient;
    private transient final String plasmaFilePath;

    private transient final ResourcePool resources = new ResourcePool();
    protected transient final ResourceScope scope = ResourceScope.newSharedScope();
    private static final long DEFAULT_REQUEST_SIZE = 1024;
    private static final ContextParameters.Feature[] FEATURE_SET = {ContextParameters.Feature.TAG, ContextParameters.Feature.RMA, ContextParameters.Feature.WAKEUP, ContextParameters.Feature.AM, ContextParameters.Feature.ATOMIC_32, ContextParameters.Feature.ATOMIC_64, ContextParameters.Feature.STREAM};
    private transient Worker worker;
    private transient Context context;
    private transient final InetSocketAddress listenAddress;

    public InfinimumDBServer(String plasmaFilePath, String listenAddress, Integer listenPort) {
        this.listenAddress = new InetSocketAddress(listenAddress, listenPort);
        this.plasmaFilePath = plasmaFilePath;
        connectPlasma();
    }

    /*public InfinimumDBServer(String plasmaFilePath, String listenAddress, Integer listeningPort,
                             String mainServerHostAddress, Integer mainServerPort) {
        this.plasmaFilePath = plasmaFilePath;
        this.infinileapServer = new InfinileapServer(listenAddress);
        this.infinileapServer.registerOnMessageEventListener(this);
        connectPlasma();
    }*/

    private void connectPlasma() {
        System.loadLibrary("plasma_java");
        try {
            this.plasmaClient = new PlasmaClient(plasmaFilePath, "", 0);
        } catch (Exception e) {
            if (log.isErrorEnabled()) log.error("PlasmaDB could not be reached");
        }
    }

    public void listen() {
        NativeLogger.enable();
        if (log.isInfoEnabled()) {
            log.info("Using UCX version {}", Context.getVersion());
        }
        try (resources) {
            initialize();
            listenLoop();
        } catch (ControlException e) {
            log.error("Native operation failed", e);
        } catch (CloseException e) {
            log.error("Closing resource failed", e);
        } catch (InterruptedException e) {
            log.error("Unexpected interrupt occurred", e);
        }
        // Release resource scope
        scope.close();
    }

    private void initialize() throws ControlException, InterruptedException {
        // Create context parameters
        var contextParameters = new ContextParameters().setFeatures(FEATURE_SET).setRequestSize(DEFAULT_REQUEST_SIZE);

        // Read configuration (Environment Variables)
        var configuration = pushResource(Configuration.read());

        log.info("Initializing context");

        // Initialize UCP context
        this.context = pushResource(Context.initialize(contextParameters, configuration));

        var workerParameters = new WorkerParameters().setThreadMode(ThreadMode.SINGLE);

        log.info("Creating worker");

        // Create a worker
        this.worker = pushResource(context.createWorker(workerParameters));

        Thread cleanUpThread = new Thread(() -> {
            if (log.isWarnEnabled()) {
                log.warn("Attempting graceful shutdown");
            }
            try {
                resources.close();
            } catch (CloseException e) {
                if (log.isErrorEnabled()) {
                    log.error("Exception while cleaning up");
                }
            } finally {
                log.warn("Success");
            }
        });
        Runtime.getRuntime().addShutdownHook(cleanUpThread);
    }

    protected <T extends AutoCloseable> T pushResource(T resource) {
        resources.push(resource);
        return resource;
    }

    private void listenLoop() throws InterruptedException, ControlException {
        var connectionRequest = new AtomicReference<ConnectionRequest>();
        var listenerParams = new ListenerParameters().setListenAddress(listenAddress).setConnectionHandler(connectionRequest::set);

        log.info("Listening for new connection requests on {}", listenAddress);
        pushResource(this.worker.createListener(listenerParams));

        while (true) {
            Requests.await(this.worker, connectionRequest);

            var endpointParameters = new EndpointParameters().setConnectionRequest(connectionRequest.get());
            // ToDo create worker pool and create endpoint from free worker
            try (Endpoint endpoint = this.worker.createEndpoint(endpointParameters)) {
                String operationName = deserialize(receiveData(OPERATION_MESSAGE_SIZE, 0L, worker, scope));
                if (log.isInfoEnabled()) {
                    log.info("Received \"{}\"", operationName);
                }
                switch (operationName) {
                    case "PUT" -> {
                        if (log.isInfoEnabled()) {
                            log.info("Start PUT operation");
                        }
                        try {
                            putOperation(worker, endpoint);
                        } catch (ControlException e) {
                            if (log.isErrorEnabled()) {
                                log.error("An exception occurred while receiving the remote key in a PUT operation.", e);
                            }
                        }
                    }
                    case "GET" -> {
                        if (log.isInfoEnabled()) {
                            log.info("Start GET operation");
                        }
                        getOperation(worker, endpoint);
                    }
                    case "DEL" -> {
                        if (log.isInfoEnabled()) {
                            log.info("Start DEL operation");
                        }
                        delOperation(worker, endpoint);
                    }
                    default -> {
                    }
                }
            } catch (ControlException e) {
                if (log.isErrorEnabled()) {
                    log.error("An exception occurred while creating an endpoint.", e);
                }
            } finally {
                connectionRequest.set(null);
            }
        }
    }

    private void delOperation(Worker worker, Endpoint endpoint) {
        String keyToDelete = receiveKey(worker, scope);
        byte[] id = generateID(keyToDelete);

        String statusCode = "404";

        if (plasmaClient.contains(id)) {
            log.info("Entry with id {} exists", id);
            PlasmaEntry entry = deserialize(plasmaClient.get(id, 100, false));

            statusCode = findAndDeleteEntryWithKey(plasmaClient, keyToDelete, entry, id);
        }

        if ("204".equals(statusCode)) {
            log.info("Object with key \"{}\" found and deleted", keyToDelete);
            sendSingleMessage(serialize("204"), 0L, endpoint, scope, worker);
        } else {
            log.warn("Object with key \"{}\" was not found in plasma store", keyToDelete);
            sendSingleMessage(serialize("404"), 0L, endpoint, scope, worker);
        }
        log.info("Del operation completed \n");
    }

    private void getOperation(Worker worker, Endpoint endpoint) throws ControlException {
        String keyToGet = receiveKey(worker, scope);
        byte[] id = generateID(keyToGet);

        PlasmaEntry entry = deserialize(plasmaClient.get(id, 1, false));

        if (keyToGet.equals(entry.key)) {
            log.info("Entry with id: {} has key: {}", id, keyToGet);
            byte[] objectBytes = entry.value;
            plasmaClient.release(id);

            sendObjectAddressAndStatusCode(objectBytes, endpoint, worker, context, scope);

            // Wait for client to signal successful transmission
            final String statusCode = deserialize(receiveData(10, 0L, worker, scope));
            log.info("Received status code \"{}\"", statusCode);
        } else {
            log.warn("Entry with id: {} has not key: {}", id, keyToGet);
            PlasmaEntry correctEntry = findEntryWithKey(plasmaClient, keyToGet, entry);

            if (correctEntry != null) {
                log.info("Found entry with key: {}", keyToGet);
                byte[] objectBytes = correctEntry.value;

                sendObjectAddressAndStatusCode(objectBytes, endpoint, worker, context, scope);

                // Wait for client to signal successful transmission
                final String statusCode = deserialize(receiveData(10, 0L, worker, scope));
                log.info("Received status code \"{}\"", statusCode);
            } else {
                log.warn("Not found entry with key: {}", keyToGet);
                sendSingleMessage(serialize("404"), 0L, endpoint, scope, worker);
            }
        }
        log.info("Get operation completed \n");
    }

    private void putOperation(Worker worker, Endpoint endpoint) throws ControlException {
        final MemoryDescriptor descriptor = receiveMemoryDescriptor(0L, worker);
        final byte[] remoteObject = receiveRemoteObject(descriptor, endpoint, worker, scope, resources);

        log.info("Read \"{}\" from remote buffer", deserialize(remoteObject).toString());

        String keyToPut = receiveKey(worker, scope);
        byte[] id = generateID(keyToPut);

        PlasmaEntry newPlasmaEntry = new PlasmaEntry(keyToPut, remoteObject, new byte[20]);
        byte[] newPlasmaEntryBytes = serialize(newPlasmaEntry);

        try {
            saveObjectToPlasma(plasmaClient, id, newPlasmaEntryBytes, new byte[0]);
            sendSingleMessage(serialize("200"), 0L, endpoint, scope, worker);
        } catch (DuplicateObjectException e) {
            log.warn(e.getMessage());
            PlasmaEntry plasmaEntry = deserialize(plasmaClient.get(id, -1, false));
            if (plasmaEntry.key.equals(newPlasmaEntry.key)) {
                log.warn("Object with id: {} has the key: {}", id, keyToPut);
                sendSingleMessage(serialize("409"), 0L, endpoint, scope, worker);
            } else {
                log.warn("Object with id: {} has not the key: {}", id, keyToPut);
                saveNewEntryToNextFreeId(plasmaClient, id, newPlasmaEntryBytes, plasmaEntry);
                sendSingleMessage(serialize("200"), 0L, endpoint, scope, worker);
            }
            plasmaClient.release(id);
        }
        log.info("Put operation completed \n");
    }
}
