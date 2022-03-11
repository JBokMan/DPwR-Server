package server;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;
import org.apache.commons.lang3.SerializationException;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
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
    private static final int CONNECTION_TIMEOUT_MS = 500;
    private static final int PLASMA_TIMEOUT_MS = 500;
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

        // Create a worker
        log.info("Creating worker");
        var workerParameters = new WorkerParameters().setThreadMode(ThreadMode.SINGLE);
        this.worker = pushResource(context.createWorker(workerParameters));

        // Creating clean up hook
        Thread cleanUpThread = new Thread(() -> {
            log.warn("Attempting graceful shutdown");
            try {
                resources.close();
                log.warn("Success");
            } catch (CloseException e) {
                log.error("Exception while cleaning up");
            }
        });
        Runtime.getRuntime().addShutdownHook(cleanUpThread);
    }

    protected <T extends AutoCloseable> T pushResource(T resource) {
        resources.push(resource);
        return resource;
    }

    private void listenLoop() throws InterruptedException, ControlException {
        final var connectionRequest = new AtomicReference<ConnectionRequest>();
        final var listenerParams = new ListenerParameters().setListenAddress(listenAddress).setConnectionHandler(connectionRequest::set);

        log.info("Listening for new connection requests on {}", listenAddress);
        pushResource(this.worker.createListener(listenerParams));
        while (true) {
            Requests.await(this.worker, connectionRequest);
            final var endpointParameters = new EndpointParameters().setConnectionRequest(connectionRequest.get()).setPeerErrorHandlingMode();
            final Endpoint endpoint = this.worker.createEndpoint(endpointParameters);
            // ToDo create worker pool and create endpoint from free worker
            try {
                final String operationName = deserialize(receiveData(OPERATION_MESSAGE_SIZE, worker, CONNECTION_TIMEOUT_MS));
                log.info("Received \"{}\"", operationName);

                switch (operationName) {
                    case "PUT" -> {
                        log.info("Start PUT operation");
                        putOperation(worker, endpoint);
                    }
                    case "GET" -> {
                        log.info("Start GET operation");
                        getOperation(worker, endpoint);
                    }
                    case "DEL" -> {
                        log.info("Start DEL operation");
                        delOperation(worker, endpoint);
                    }
                }
            } catch (SerializationException | ExecutionException | TimeoutException | CloseException e) {
                log.error(e.getMessage());
            } finally {
                endpoint.closeNonBlocking();
                connectionRequest.set(null);
            }
        }
    }

    private void putOperation(final Worker worker, final Endpoint endpoint) throws TimeoutException, ControlException {
        final String keyToPut = receiveKey(worker, CONNECTION_TIMEOUT_MS);
        final byte[] id = generateID(keyToPut);

        final byte[] remoteObject = receiveRemoteObject(endpoint, worker, CONNECTION_TIMEOUT_MS);

        final PlasmaEntry newPlasmaEntry = new PlasmaEntry(keyToPut, remoteObject, new byte[20]);
        final byte[] newPlasmaEntryBytes = serialize(newPlasmaEntry);

        String statusCode = "200";
        try {
            saveObjectToPlasma(plasmaClient, id, newPlasmaEntryBytes, new byte[0]);
        } catch (DuplicateObjectException e) {
            log.warn(e.getMessage());
            final PlasmaEntry plasmaEntry = deserialize(plasmaClient.get(id, PLASMA_TIMEOUT_MS, false));
            if (plasmaEntry.key.equals(newPlasmaEntry.key)) {
                log.warn("Object with id: {} has the key: {}", id, keyToPut);
                statusCode = "409";
            } else {
                log.info("Object with id: {} has not the key: {}", id, keyToPut);
                try {
                    saveNewEntryToNextFreeId(plasmaClient, id, keyToPut, newPlasmaEntryBytes, plasmaEntry, PLASMA_TIMEOUT_MS);
                } catch (DuplicateObjectException e2) {
                    log.warn(e2.getMessage());
                    statusCode = "409";
                }
            }
        }
        sendSingleMessage(serialize(statusCode), endpoint, worker, CONNECTION_TIMEOUT_MS);
        log.info("Put operation completed \n");
    }

    private void getOperation(final Worker worker, final Endpoint endpoint) throws ControlException, ExecutionException, TimeoutException, CloseException {
        final String keyToGet = receiveKey(worker, CONNECTION_TIMEOUT_MS);
        final byte[] id = generateID(keyToGet);

        final ByteBuffer objectBuffer = plasmaClient.getObjAsByteBuffer(id, PLASMA_TIMEOUT_MS, false);
        final PlasmaEntry entry = getPlasmaEntryFromBuffer(objectBuffer);

        if (keyToGet.equals(entry.key)) {
            log.info("Entry with id: {} has key: {}", id, keyToGet);

            sendObjectAddressAndStatusCode(objectBuffer, endpoint, worker, context, CONNECTION_TIMEOUT_MS);

            // Wait for client to signal successful transmission
            final String statusCode = deserialize(receiveData(10, worker, CONNECTION_TIMEOUT_MS));
            plasmaClient.release(id);
            log.info("Received status code \"{}\"", statusCode);
        } else {
            log.warn("Entry with id: {} has not key: {}", id, keyToGet);
            final ByteBuffer bufferOfCorrectEntry = findEntryWithKey(plasmaClient, keyToGet, objectBuffer, PLASMA_TIMEOUT_MS);

            if (bufferOfCorrectEntry != null) {
                log.info("Found entry with key: {}", keyToGet);

                sendObjectAddressAndStatusCode(bufferOfCorrectEntry, endpoint, worker, context, CONNECTION_TIMEOUT_MS);

                // Wait for client to signal successful transmission
                final String statusCode = deserialize(receiveData(10, worker, CONNECTION_TIMEOUT_MS));
                log.info("Received status code \"{}\"", statusCode);
            } else {
                log.warn("Not found entry with key: {}", keyToGet);
                sendSingleMessage(serialize("404"), endpoint, worker, CONNECTION_TIMEOUT_MS);
            }
        }
        log.info("Get operation completed \n");
    }

    private void delOperation(final Worker worker, final Endpoint endpoint) throws ExecutionException, TimeoutException {
        final String keyToDelete = receiveKey(worker, CONNECTION_TIMEOUT_MS);
        final byte[] id = generateID(keyToDelete);

        String statusCode = "404";

        if (plasmaClient.contains(id)) {
            log.info("Entry with id {} exists", id);
            final PlasmaEntry entry = deserialize(plasmaClient.get(id, PLASMA_TIMEOUT_MS, false));

            statusCode = findAndDeleteEntryWithKey(plasmaClient, keyToDelete, entry, id, PLASMA_TIMEOUT_MS);
        }

        if ("204".equals(statusCode)) {
            log.info("Object with key \"{}\" found and deleted", keyToDelete);
            sendSingleMessage(serialize("204"), endpoint, worker, CONNECTION_TIMEOUT_MS);
        } else {
            log.warn("Object with key \"{}\" was not found in plasma store", keyToDelete);
            sendSingleMessage(serialize("404"), endpoint, worker, CONNECTION_TIMEOUT_MS);
        }
        log.info("Del operation completed \n");
    }
}
