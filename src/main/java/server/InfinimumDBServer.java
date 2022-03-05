package server;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
    }

    private void initialize() throws ControlException, InterruptedException {
        NativeLogger.enable();
        log.info("Using UCX version {}", Context.getVersion());

        // Create context parameters
        var contextParameters = new ContextParameters().setFeatures(FEATURE_SET).setRequestSize(DEFAULT_REQUEST_SIZE);

        // Read configuration (Environment Variables)
        var configuration = pushResource(Configuration.read());

        // Initialize UCP context
        log.info("Initializing context");
        this.context = pushResource(Context.initialize(contextParameters, configuration));
        var workerParameters = new WorkerParameters().setThreadMode(ThreadMode.SINGLE);

        // Create a worker
        log.info("Creating worker");
        this.worker = pushResource(context.createWorker(workerParameters));

        // Creating clean up hook
        Thread cleanUpThread = new Thread(() -> {
            log.warn("Attempting graceful shutdown");
            try {
                resources.close();
            } catch (CloseException e) {
                log.error("Exception while cleaning up");
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
                String operationName = deserialize(receiveData(OPERATION_MESSAGE_SIZE, 0L, worker));
                log.info("Received \"{}\"", operationName);

                switch (operationName) {
                    case "PUT" -> {
                        log.info("Start PUT operation");
                        try {
                            putOperation(worker, endpoint);
                        } catch (ControlException e) {
                            log.error("An exception occurred while receiving the remote key in a PUT operation.", e);
                        }
                    }
                    case "GET" -> {
                        log.info("Start GET operation");
                        try {
                            getOperation(worker, endpoint);
                        } catch (ControlException e) {
                            log.error("An exception occurred while sending the remote key in a GET operation.", e);
                        }
                    }
                    case "DEL" -> {
                        log.info("Start DEL operation");
                        delOperation(worker, endpoint);
                    }
                }
            } catch (ControlException e) {
                log.error("An exception occurred while creating an endpoint.", e);
            } finally {
                connectionRequest.set(null);
            }
        }
    }

    private void putOperation(Worker worker, Endpoint endpoint) throws ControlException {
        String keyToPut = receiveKey(worker);
        byte[] id = generateID(keyToPut);

        final byte[] remoteObject = receiveRemoteObject(endpoint, worker);
        log.info("Read \"{}\" from remote buffer", deserialize(remoteObject).toString());

        PlasmaEntry newPlasmaEntry = new PlasmaEntry(keyToPut, remoteObject, new byte[20]);
        byte[] newPlasmaEntryBytes = serialize(newPlasmaEntry);

        String statusCode = "200";
        try {
            saveObjectToPlasma(plasmaClient, id, newPlasmaEntryBytes, new byte[0]);
        } catch (DuplicateObjectException e) {
            log.warn(e.getMessage());
            PlasmaEntry plasmaEntry = deserialize(plasmaClient.get(id, -1, false));
            if (plasmaEntry.key.equals(newPlasmaEntry.key)) {
                log.warn("Object with id: {} has the key: {}", id, keyToPut);
                statusCode = "409";
            } else {
                log.info("Object with id: {} has not the key: {}", id, keyToPut);
                try {
                    saveNewEntryToNextFreeId(plasmaClient, id, keyToPut, newPlasmaEntryBytes, plasmaEntry);
                } catch (DuplicateObjectException e2) {
                    log.warn(e2.getMessage());
                    statusCode = "409";
                }
            }
            plasmaClient.release(id);
        }
        sendSingleMessage(serialize(statusCode), 0L, endpoint, worker);
        log.info("Put operation completed \n");
    }

    private void getOperation(Worker worker, Endpoint endpoint) throws ControlException {
        String keyToGet = receiveKey(worker);
        byte[] id = generateID(keyToGet);

        ByteBuffer objectBuffer = plasmaClient.getObjAsByteBuffer(id, 1, false);
        PlasmaEntry entry = getPlasmaEntryFromBuffer(objectBuffer);

        if (keyToGet.equals(entry.key)) {
            log.info("Entry with id: {} has key: {}", id, keyToGet);

            sendObjectAddressAndStatusCode(objectBuffer, endpoint, worker, context);

            // Wait for client to signal successful transmission
            final String statusCode = deserialize(receiveData(10, 0L, worker));
            plasmaClient.release(id);
            log.info("Received status code \"{}\"", statusCode);
        } else {
            log.warn("Entry with id: {} has not key: {}", id, keyToGet);
            ByteBuffer bufferOfCorrectEntry = findEntryWithKey(plasmaClient, keyToGet, objectBuffer);

            if (bufferOfCorrectEntry != null) {
                log.info("Found entry with key: {}", keyToGet);

                sendObjectAddressAndStatusCode(bufferOfCorrectEntry, endpoint, worker, context);

                // Wait for client to signal successful transmission
                final String statusCode = deserialize(receiveData(10, 0L, worker));
                log.info("Received status code \"{}\"", statusCode);
            } else {
                log.warn("Not found entry with key: {}", keyToGet);
                sendSingleMessage(serialize("404"), 0L, endpoint, worker);
            }
        }
        log.info("Get operation completed \n");
    }

    private void delOperation(Worker worker, Endpoint endpoint) {
        String keyToDelete = receiveKey(worker);
        byte[] id = generateID(keyToDelete);

        String statusCode = "404";

        if (plasmaClient.contains(id)) {
            log.info("Entry with id {} exists", id);
            PlasmaEntry entry = deserialize(plasmaClient.get(id, 100, false));

            statusCode = findAndDeleteEntryWithKey(plasmaClient, keyToDelete, entry, id);
        }

        if ("204".equals(statusCode)) {
            log.info("Object with key \"{}\" found and deleted", keyToDelete);
            sendSingleMessage(serialize("204"), 0L, endpoint, worker);
        } else {
            log.warn("Object with key \"{}\" was not found in plasma store", keyToDelete);
            sendSingleMessage(serialize("404"), 0L, endpoint, worker);
        }
        log.info("Del operation completed \n");
    }
}
