package server;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.commons.lang3.ArrayUtils;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;
import static utils.CommunicationUtils.*;
import static utils.HashUtils.generateID;
import static utils.HashUtils.generateNextIdOfId;
import static utils.PlasmaUtils.*;

@Slf4j
public class InfinimumDBServer {

    private static final int OPERATION_MESSAGE_SIZE = 10;
    static final int serverID = 0;
    int serverCount = 1;

    private PlasmaClient plasmaClient;
    private final String plasmaFilePath;

    private final ResourcePool resources = new ResourcePool();
    private static final long DEFAULT_REQUEST_SIZE = 1024L;
    private static final ContextParameters.Feature[] FEATURE_SET = {ContextParameters.Feature.TAG, ContextParameters.Feature.RMA, ContextParameters.Feature.WAKEUP, ContextParameters.Feature.AM, ContextParameters.Feature.ATOMIC_32, ContextParameters.Feature.ATOMIC_64, ContextParameters.Feature.STREAM};
    private static final int CONNECTION_TIMEOUT_MS = 750;
    private static final int PLASMA_TIMEOUT_MS = 500;
    private Worker worker;
    private Context context;
    private final InetSocketAddress listenAddress;
    private final SafeCounterWithoutLock runningTagID = new SafeCounterWithoutLock();
    private ExecutorService executorService;

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
        } catch (final Exception e) {
            if (log.isErrorEnabled()) log.error("PlasmaDB could not be reached");
        }
    }

    public void listen() {
        try (resources) {
            initialize();
            listenLoop();
        } catch (final ControlException e) {
            log.error("Native operation failed", e);
        } catch (final CloseException e) {
            log.error("Closing resource failed", e);
        } catch (final InterruptedException e) {
            log.error("Unexpected interrupt occurred", e);
        }
    }

    private void initialize() throws ControlException, InterruptedException {
        NativeLogger.enable();
        log.info("Using UCX version {}", Context.getVersion());

        // Create context parameters
        final var contextParameters = new ContextParameters().setFeatures(FEATURE_SET).setRequestSize(DEFAULT_REQUEST_SIZE);

        // Read configuration (Environment Variables)
        final var configuration = pushResource(Configuration.read());

        // Initialize UCP context
        log.info("Initializing context");
        this.context = pushResource(Context.initialize(contextParameters, configuration));

        // Create a worker
        log.info("Creating worker");
        final var workerParameters = new WorkerParameters().setThreadMode(ThreadMode.SINGLE);
        this.worker = pushResource(context.createWorker(workerParameters));

        executorService = Executors.newFixedThreadPool(2);

        // Creating clean up hook
        final Thread cleanUpThread = new Thread(() -> {
            log.warn("Attempting graceful shutdown");
            try {
                resources.close();
                log.warn("Success");
            } catch (final CloseException e) {
                log.error("Exception while cleaning up");
            }
        });
        Runtime.getRuntime().addShutdownHook(cleanUpThread);
    }

    protected <T extends AutoCloseable> T pushResource(final T resource) {
        resources.push(resource);
        return resource;
    }

    private void listenLoop() throws InterruptedException, ControlException {
        final var connectionQueue = new LinkedBlockingQueue<ConnectionRequest>();
        final var listenerParams = new ListenerParameters().setListenAddress(listenAddress).setConnectionHandler(connectionQueue::add);
        log.info("Listening for new connection requests on {}", listenAddress);
        pushResource(this.worker.createListener(listenerParams));

        while (true) {
            Requests.await(this.worker, connectionQueue);
            while (!connectionQueue.isEmpty()) {
                final ConnectionRequest request = connectionQueue.remove();
                executorService.submit(() -> {
                    try {
                        handleRequest(request);
                    } catch (final ControlException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }

    private void handleRequest(final ConnectionRequest request) throws ControlException {
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final var workerParameters = new WorkerParameters(scope).setThreadMode(ThreadMode.SINGLE);
            final Worker currentWorker = context.createWorker(workerParameters);
            final var endpointParameters = new EndpointParameters(scope).setConnectionRequest(request).setPeerErrorHandlingMode();
            final Endpoint endpoint = currentWorker.createEndpoint(endpointParameters);
            final int tagID = this.runningTagID.getValue();
            this.runningTagID.increment();
            try {
                final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(tagID);
                sendSingleMessage(tagID, byteBuffer.array(), endpoint, currentWorker, CONNECTION_TIMEOUT_MS);
                final String operationName = deserialize(receiveData(tagID, OPERATION_MESSAGE_SIZE, currentWorker, CONNECTION_TIMEOUT_MS));
                log.info("Received \"{}\"", operationName);

                switch (operationName) {
                    case "PUT" -> {
                        log.info("Start PUT operation");
                        putOperation(tagID, currentWorker, endpoint);
                    }
                    case "GET" -> {
                        log.info("Start GET operation");
                        getOperation(tagID, currentWorker, endpoint);
                    }
                    case "DEL" -> {
                        log.info("Start DEL operation");
                        delOperation(tagID, currentWorker, endpoint);
                    }
                }
            } catch (final CloseException | TimeoutException | ExecutionException | ControlException e) {
                log.error(e.getMessage());
            } finally {
                endpoint.close();
                currentWorker.close();
            }
        }
    }

    private void putOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws TimeoutException, ControlException, CloseException {
        final String keyToPut = receiveKey(tagID, worker, CONNECTION_TIMEOUT_MS);
        byte[] id = generateID(keyToPut);
        final int entrySize = receiveEntrySize(tagID, worker, CONNECTION_TIMEOUT_MS);

        if (plasmaClient.contains(id)) {
            log.warn("Plasma does contain the id");
            final PlasmaEntry plasmaEntry = deserialize(plasmaClient.get(id, PLASMA_TIMEOUT_MS, false));
            plasmaClient.release(id);
            final byte[] objectIdWithFreeNextID = getObjectIdOfNextEntryWithEmptyNextID(plasmaClient, plasmaEntry, id, keyToPut, PLASMA_TIMEOUT_MS);

            if (ArrayUtils.isEmpty(objectIdWithFreeNextID)) {
                log.warn("Object with key is already in plasma");
                sendSingleMessage(tagID, serialize("409"), endpoint, worker, CONNECTION_TIMEOUT_MS);
                log.info("Put operation completed \n");
                return;
            }

            log.warn("Key is not in plasma, handling id collision");
            id = generateNextIdOfId(objectIdWithFreeNextID);

            sendNewEntryAddress(tagID, plasmaClient, id, entrySize, endpoint, worker, context, CONNECTION_TIMEOUT_MS);
            awaitPutCompletionSignal(tagID, plasmaClient, id, worker, objectIdWithFreeNextID, CONNECTION_TIMEOUT_MS, PLASMA_TIMEOUT_MS);
        } else {
            log.info("Plasma does not contain the id");
            sendNewEntryAddress(tagID, plasmaClient, id, entrySize, endpoint, worker, context, CONNECTION_TIMEOUT_MS);
            awaitPutCompletionSignal(tagID, plasmaClient, id, worker, null, CONNECTION_TIMEOUT_MS, PLASMA_TIMEOUT_MS);
        }
        log.info("Put operation completed \n");
    }

    private void getOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws ControlException, ExecutionException, TimeoutException, CloseException {
        final String keyToGet = receiveKey(tagID, worker, CONNECTION_TIMEOUT_MS);
        final byte[] id = generateID(keyToGet);

        final ByteBuffer objectBuffer = plasmaClient.getObjAsByteBuffer(id, PLASMA_TIMEOUT_MS, false);
        final PlasmaEntry entry = getPlasmaEntryFromBuffer(objectBuffer);

        if (keyToGet.equals(entry.key)) {
            log.info("Entry with id: {} has key: {}", id, keyToGet);

            sendObjectAddressAndStatusCode(tagID, objectBuffer, endpoint, worker, context, CONNECTION_TIMEOUT_MS);

            // Wait for client to signal successful transmission
            final String statusCode = deserialize(receiveData(tagID, 10, worker, CONNECTION_TIMEOUT_MS));
            plasmaClient.release(id);
            log.info("Received status code \"{}\"", statusCode);
        } else {
            log.warn("Entry with id: {} has not key: {}", id, keyToGet);
            final ByteBuffer bufferOfCorrectEntry = findEntryWithKey(plasmaClient, keyToGet, objectBuffer, PLASMA_TIMEOUT_MS);

            if (bufferOfCorrectEntry != null) {
                log.info("Found entry with key: {}", keyToGet);

                sendObjectAddressAndStatusCode(tagID, bufferOfCorrectEntry, endpoint, worker, context, CONNECTION_TIMEOUT_MS);

                // Wait for client to signal successful transmission
                final String statusCode = deserialize(receiveData(tagID, 10, worker, CONNECTION_TIMEOUT_MS));
                log.info("Received status code \"{}\"", statusCode);
            } else {
                log.warn("Not found entry with key: {}", keyToGet);
                sendSingleMessage(tagID, serialize("404"), endpoint, worker, CONNECTION_TIMEOUT_MS);
            }
        }
        log.info("Get operation completed \n");
    }

    private void delOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws ExecutionException, TimeoutException {
        final String keyToDelete = receiveKey(tagID, worker, CONNECTION_TIMEOUT_MS);
        final byte[] id = generateID(keyToDelete);

        String statusCode = "404";

        if (plasmaClient.contains(id)) {
            log.info("Entry with id {} exists", id);
            final PlasmaEntry entry = deserialize(plasmaClient.get(id, PLASMA_TIMEOUT_MS, false));
            log.info(entry.toString());
            statusCode = findAndDeleteEntryWithKey(plasmaClient, keyToDelete, entry, id, PLASMA_TIMEOUT_MS);
        }

        if ("204".equals(statusCode)) {
            log.info("Object with key \"{}\" found and deleted", keyToDelete);
            sendSingleMessage(tagID, serialize("204"), endpoint, worker, CONNECTION_TIMEOUT_MS);
        } else {
            log.warn("Object with key \"{}\" was not found in plasma store", keyToDelete);
            sendSingleMessage(tagID, serialize("404"), endpoint, worker, CONNECTION_TIMEOUT_MS);
        }
        log.info("Del operation completed \n");
    }
}

class SafeCounterWithoutLock {
    private final AtomicInteger counter = new AtomicInteger(0);

    public int getValue() {
        return counter.get();
    }

    public void increment() {
        while (true) {
            final int existingValue = getValue();
            final int newValue = existingValue + 1;
            if (counter.compareAndSet(existingValue, newValue)) {
                return;
            }
        }
    }
}