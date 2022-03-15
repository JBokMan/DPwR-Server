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
import org.apache.commons.lang3.SerializationUtils;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;
import static utils.CommunicationUtils.*;
import static utils.HashUtils.generateID;
import static utils.HashUtils.generateTailEndOfNextIdOfId;
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
    private static final int CONNECTION_TIMEOUT_MS = 750;
    private static final int PLASMA_TIMEOUT_MS = 500;
    private transient Worker worker;
    private transient Context context;
    private transient final InetSocketAddress listenAddress;

    public InfinimumDBServer(final String plasmaFilePath, final String listenAddress, final Integer listenPort) {
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

    private void putOperation(final Worker worker, final Endpoint endpoint) throws TimeoutException, ControlException, CloseException {
        final String keyToPut = receiveKey(worker, CONNECTION_TIMEOUT_MS);
        final byte[] id = generateID(keyToPut);
        final byte[] entrySizeBytes = receiveData(Integer.BYTES, worker, CONNECTION_TIMEOUT_MS);
        ByteBuffer byteBuffer = ByteBuffer.wrap(entrySizeBytes);
        int entrySize = byteBuffer.getInt();
        log.info("Received \"{}\"", entrySize);
        if (plasmaClient.contains(id)) {
            final PlasmaEntry plasmaEntry = deserialize(plasmaClient.get(id, PLASMA_TIMEOUT_MS, false));
            plasmaClient.release(id);
            if (plasmaEntry.key.equals(keyToPut)) {
                log.warn("Object with id: {} has the key: {}", id, keyToPut);
                sendSingleMessage(serialize("409"), endpoint, worker, CONNECTION_TIMEOUT_MS);
            } else {
                log.info("Object with id: {} has not the key: {}", id, keyToPut);
                sendSingleMessage(serialize("408"), endpoint, worker, CONNECTION_TIMEOUT_MS);
                try {
                    final byte[] objectIdWithFreeNextID = getObjectIdOfNextEntryWithEmptyNextID(plasmaClient, plasmaEntry, id, keyToPut, PLASMA_TIMEOUT_MS);
                    log.info("Next object id with free next id is: {}", objectIdWithFreeNextID);
                    final PlasmaEntry plasmaEntryWithEmptyNextID = deserialize(plasmaClient.get(objectIdWithFreeNextID, PLASMA_TIMEOUT_MS, false));
                    final byte[] newTailEnd = generateTailEndOfNextIdOfId(objectIdWithFreeNextID);
                    log.info(String.valueOf(newTailEnd.length));
                    sendSingleMessage(newTailEnd, endpoint, worker, CONNECTION_TIMEOUT_MS);
                    byteBuffer = plasmaClient.create(id, entrySize, new byte[0]);
                    final MemoryDescriptor objectAddress;
                    try {
                        objectAddress = getMemoryDescriptorOfByteBuffer(byteBuffer, context);
                    } catch (ControlException | CloseException e) {
                        log.error("An exception occurred getting the objects memory address, aborting GET operation");
                        sendSingleMessage(serialize("500"), endpoint, worker, CONNECTION_TIMEOUT_MS);
                        throw e;
                    }
                    final ArrayList<Long> requests = new ArrayList<>();
                    requests.add(prepareToSendRemoteKey(objectAddress, endpoint));
                    sendData(requests, worker, CONNECTION_TIMEOUT_MS);
                    final String statusCode = SerializationUtils.deserialize(receiveData(10, worker, CONNECTION_TIMEOUT_MS));
                    log.info("Received status code: \"{}\"", statusCode);
                    if ("200".equals(statusCode)) {
                        plasmaClient.seal(id);
                        deleteById(objectIdWithFreeNextID, plasmaClient);
                        final PlasmaEntry updatedEntry = new PlasmaEntry(plasmaEntryWithEmptyNextID.key, plasmaEntryWithEmptyNextID.value, id);
                        saveObjectToPlasma(plasmaClient, objectIdWithFreeNextID, serialize(updatedEntry), new byte[0]);
                    }
                } catch (DuplicateObjectException e) {
                    log.warn(e.getMessage());
                    sendSingleMessage(serialize("409"), endpoint, worker, CONNECTION_TIMEOUT_MS);
                }
            }
        } else {
            sendSingleMessage(serialize("200"), endpoint, worker, CONNECTION_TIMEOUT_MS);
            byteBuffer = plasmaClient.create(id, entrySize, new byte[0]);
            final MemoryDescriptor objectAddress;
            try {
                objectAddress = getMemoryDescriptorOfByteBuffer(byteBuffer, context);
            } catch (ControlException | CloseException e) {
                log.error("An exception occurred getting the objects memory address, aborting GET operation");
                sendSingleMessage(serialize("500"), endpoint, worker, CONNECTION_TIMEOUT_MS);
                throw e;
            }
            final ArrayList<Long> requests = new ArrayList<>();
            requests.add(prepareToSendRemoteKey(objectAddress, endpoint));
            sendData(requests, worker, CONNECTION_TIMEOUT_MS);
            final String statusCode = SerializationUtils.deserialize(receiveData(10, worker, CONNECTION_TIMEOUT_MS));
            log.info("Received status code: \"{}\"", statusCode);
            if ("200".equals(statusCode)) {
                plasmaClient.seal(id);
            }
        }
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
