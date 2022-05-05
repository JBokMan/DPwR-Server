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
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import utils.DPwRErrorHandler;
import utils.WorkerPool;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static utils.CommunicationUtils.*;
import static utils.HashUtils.generateID;
import static utils.HashUtils.generateNextIdOfId;
import static utils.PlasmaUtils.*;

@Slf4j
public class DPwRServer {

    int serverID = -1;
    private final AtomicInteger serverCount = new AtomicInteger(1);
    private final Map<Integer, InetSocketAddress> serverMap = new HashMap<>();

    private PlasmaClient plasmaClient;
    private final String plasmaFilePath;

    private final ResourcePool resources = new ResourcePool();
    private static final ErrorHandler errorHandler = new DPwRErrorHandler();
    private static final ContextParameters.Feature[] FEATURE_SET = {ContextParameters.Feature.TAG, ContextParameters.Feature.RMA, ContextParameters.Feature.WAKEUP};
    private static final int CONNECTION_TIMEOUT_MS = 750;
    private static final int PLASMA_TIMEOUT_MS = 500;
    private Worker worker;
    private Context context;
    private final InetSocketAddress listenAddress;
    private final AtomicInteger runningTagID = new AtomicInteger(0);
    private ExecutorService executorService;
    @SuppressWarnings("FieldCanBeLocal")
    // listener parameter need to stay in memory, since it holds the callback for new connection requests
    private ListenerParameters listenerParameters;
    private WorkerPool workerPool;

    public DPwRServer(final String plasmaFilePath, final String listenAddress, final Integer listenPort) {
        this.listenAddress = new InetSocketAddress(listenAddress, listenPort);
        this.plasmaFilePath = plasmaFilePath;
        connectPlasma();
        this.serverID = 0;
        serverMap.put(0, this.listenAddress);
    }

    public DPwRServer(final String plasmaFilePath, final String listenAddress, final Integer listenPort, final String mainServerHostAddress, final Integer mainServerPort) throws ControlException, TimeoutException, ConnectException {
        this.listenAddress = new InetSocketAddress(listenAddress, listenPort);
        this.plasmaFilePath = plasmaFilePath;
        connectPlasma();
        registerServer(new InetSocketAddress(mainServerHostAddress, mainServerPort));
    }

    private void connectPlasma() {
        System.loadLibrary("plasma_java");
        try {
            this.plasmaClient = new PlasmaClient(plasmaFilePath, "", 0);
        } catch (final Exception e) {
            if (log.isErrorEnabled()) log.error("PlasmaDB could not be reached");
        }
    }

    private void registerServer(final InetSocketAddress mainServerAddress) throws ControlException, TimeoutException, ConnectException {
        try (final ResourcePool resourcePool = new ResourcePool(); final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final ContextParameters contextParameters = new ContextParameters(scope).setFeatures(ContextParameters.Feature.TAG);
            final Context context = Context.initialize(contextParameters, null);
            resourcePool.push(context);

            final WorkerParameters workerParameters = new WorkerParameters(scope).setThreadMode(ThreadMode.SINGLE);
            final Worker worker = context.createWorker(workerParameters);
            resourcePool.push(worker);

            registerAtMain(mainServerAddress, worker, scope);

            for (final Map.Entry<Integer, InetSocketAddress> entry : this.serverMap.entrySet()) {
                final int serverID = entry.getKey();
                if (serverID == 0 || serverID == this.serverID) {
                    continue;
                }
                final InetSocketAddress address = entry.getValue();
                registerAtSecondary(address, worker, scope);
            }
        } catch (final CloseException e) {
            log.error(e.getMessage());
        }

        log.info(String.valueOf(this.serverID));
        for (final var entry : this.serverMap.entrySet()) {
            log.info(entry.getKey() + "/" + entry.getValue());
        }
    }

    private void registerAtMain(final InetSocketAddress mainServerAddress, final Worker worker, final ResourceScope scope) throws ControlException, TimeoutException, ConnectException {
        log.info("Register at main: {}", mainServerAddress);

        final EndpointParameters endpointParams = new EndpointParameters(scope).setRemoteAddress(mainServerAddress).setErrorHandler(errorHandler);
        final Endpoint endpoint = worker.createEndpoint(endpointParams);

        final int tagID = receiveTagID(worker, CONNECTION_TIMEOUT_MS);
        sendOperationName(tagID, "REG", endpoint, worker, CONNECTION_TIMEOUT_MS);
        sendAddress(tagID, this.listenAddress, endpoint, worker, CONNECTION_TIMEOUT_MS);

        final String statusCode = receiveStatusCode(tagID, worker, CONNECTION_TIMEOUT_MS);

        if ("200".equals(statusCode)) {
            final int serverCount = receiveInteger(tagID, worker, CONNECTION_TIMEOUT_MS);
            this.serverCount.set(serverCount);
            this.serverID = serverCount - 1;

            for (int i = 0; i < serverCount; i++) {
                final InetSocketAddress serverAddress = receiveAddress(tagID, worker, CONNECTION_TIMEOUT_MS);
                serverMap.put(i, serverAddress);
            }
            serverMap.put(this.serverID, this.listenAddress);
        } else if ("206".equals(statusCode)) {
            throw new ConnectException("Given address was of a secondary server and not of the main server");
        } else if ("400".equals(statusCode)) {
            throw new ConnectException("This server was already registered");
        } else {
            throw new ConnectException("Main server could not be reached");
        }
        endpoint.close();
    }

    private void registerAtSecondary(final InetSocketAddress address, final Worker worker, final ResourceScope scope) throws TimeoutException, ConnectException, ControlException {
        log.info("Register at secondary: {}", address);

        final EndpointParameters endpointParams = new EndpointParameters(scope).setRemoteAddress(address).setErrorHandler(errorHandler);
        final Endpoint endpoint = worker.createEndpoint(endpointParams);

        final int tagID = receiveTagID(worker, CONNECTION_TIMEOUT_MS);
        sendOperationName(tagID, "REG", endpoint, worker, CONNECTION_TIMEOUT_MS);
        sendAddress(tagID, this.listenAddress, endpoint, worker, CONNECTION_TIMEOUT_MS);

        final String statusCode = receiveStatusCode(tagID, worker, CONNECTION_TIMEOUT_MS);

        if ("206".equals(statusCode)) {
            sendSingleInteger(tagID, this.serverID, endpoint, worker, CONNECTION_TIMEOUT_MS);
        } else if ("200".equals(statusCode)) {
            throw new ConnectException("Given address was of the main server and not of the secondary server");
        } else if ("400".equals(statusCode)) {
            throw new ConnectException("This server was already registered");
        } else {
            throw new ConnectException("Secondary server could not be reached");
        }
        endpoint.close();
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

    private void initialize() throws ControlException {
        NativeLogger.enable();
        log.info("Using UCX version {}", Context.getVersion());

        // Initialize UCP context
        log.info("Initializing context");
        final ContextParameters contextParameters = new ContextParameters().setFeatures(FEATURE_SET);
        this.context = pushResource(Context.initialize(contextParameters, null));

        // Create a worker
        log.info("Creating worker");
        final WorkerParameters workerParameters = new WorkerParameters().setThreadMode(ThreadMode.SINGLE);
        this.worker = pushResource(context.createWorker(workerParameters));

        // worker pool count must be greater than thread count since the endpoint closes not fast enough
        this.workerPool = new WorkerPool(256, workerParameters, context);
        this.executorService = Executors.newFixedThreadPool(8);

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

    @SuppressWarnings("InfiniteLoopStatement")
    private void listenLoop() throws ControlException, InterruptedException {
        final LinkedBlockingQueue<ConnectionRequest> connectionQueue = new LinkedBlockingQueue<>();

        log.info("Listening for new connection requests on {}", listenAddress);
        this.listenerParameters = new ListenerParameters().setListenAddress(listenAddress).setConnectionHandler(connectionQueue::add);
        pushResource(this.worker.createListener(this.listenerParameters));

        while (true) {
            Requests.await(this.worker, connectionQueue);
            while (!connectionQueue.isEmpty()) {
                final ConnectionRequest request = connectionQueue.remove();
                final Worker currentWorker = this.workerPool.getNextWorker();
                executorService.submit(() -> {
                    try {
                        handleRequest(request, currentWorker);
                    } catch (final ControlException | CloseException | TimeoutException e) {
                        log.error(e.getMessage());
                    }
                });
            }
        }
    }

    private void handleRequest(final ConnectionRequest request, final Worker currentWorker) throws ControlException, TimeoutException, CloseException {
        try (final ResourceScope scope = ResourceScope.newConfinedScope(); final Endpoint endpoint = currentWorker.createEndpoint(new EndpointParameters(scope).setConnectionRequest(request).setErrorHandler(errorHandler))) {
            // Send tagID to client and increment
            final int tagID = this.runningTagID.getAndIncrement();
            sendSingleInteger(0, tagID, endpoint, currentWorker, CONNECTION_TIMEOUT_MS);

            final String operationName = receiveOperationName(tagID, currentWorker, CONNECTION_TIMEOUT_MS);

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
                case "REG" -> {
                    log.info("Start REG operation");
                    regOperation(tagID, currentWorker, endpoint);
                }
                case "INF" -> {
                    log.info("Start INF operation");
                    infOperation(tagID, currentWorker, endpoint);
                }
            }
        }
    }

    private void putOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws TimeoutException, ControlException, CloseException {
        final String keyToPut = receiveKey(tagID, worker, CONNECTION_TIMEOUT_MS);
        byte[] id = generateID(keyToPut);
        final int entrySize = receiveInteger(tagID, worker, CONNECTION_TIMEOUT_MS);

        if (plasmaClient.contains(id)) {
            log.warn("Plasma does contain the id");
            final PlasmaEntry plasmaEntry = getPlasmaEntry(plasmaClient, id, PLASMA_TIMEOUT_MS);
            final byte[] objectIdWithFreeNextID = getObjectIdOfNextEntryWithEmptyNextID(plasmaClient, plasmaEntry, id, keyToPut, PLASMA_TIMEOUT_MS);

            if (ArrayUtils.isEmpty(objectIdWithFreeNextID)) {
                log.warn("Object with key is already in plasma");
                sendStatusCode(tagID, "409", endpoint, worker, CONNECTION_TIMEOUT_MS);
            } else {
                log.warn("Key is not in plasma, handling id collision");
                id = generateNextIdOfId(objectIdWithFreeNextID);

                createEntryAndSendNewEntryAddress(tagID, plasmaClient, id, entrySize, endpoint, worker, context, CONNECTION_TIMEOUT_MS);
                awaitPutCompletionSignal(tagID, plasmaClient, id, worker, objectIdWithFreeNextID, CONNECTION_TIMEOUT_MS, PLASMA_TIMEOUT_MS);
            }
        } else {
            log.info("Plasma does not contain the id");
            createEntryAndSendNewEntryAddress(tagID, plasmaClient, id, entrySize, endpoint, worker, context, CONNECTION_TIMEOUT_MS);
            awaitPutCompletionSignal(tagID, plasmaClient, id, worker, null, CONNECTION_TIMEOUT_MS, PLASMA_TIMEOUT_MS);
        }
        log.info("Put operation completed");
    }

    private void getOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws ControlException, TimeoutException, CloseException {
        final String keyToGet = receiveKey(tagID, worker, CONNECTION_TIMEOUT_MS);
        final byte[] id = generateID(keyToGet);

        ByteBuffer entryBuffer = null;

        if (plasmaClient.contains(id)) {
            entryBuffer = plasmaClient.getObjAsByteBuffer(id, PLASMA_TIMEOUT_MS, false);
            final PlasmaEntry entry = getPlasmaEntryFromBuffer(entryBuffer);

            if (!StringUtils.equals(keyToGet, entry.key)) {
                log.warn("Entry with id: {} has not key: {}", id, keyToGet);
                entryBuffer = findEntryWithKey(plasmaClient, keyToGet, entryBuffer, PLASMA_TIMEOUT_MS);
            }
        }

        if (ObjectUtils.isNotEmpty(entryBuffer)) {
            sendStatusCode(tagID, "200", endpoint, worker, CONNECTION_TIMEOUT_MS);
            sendObjectAddress(tagID, entryBuffer, endpoint, worker, context, CONNECTION_TIMEOUT_MS);
            // Wait for client to signal successful transmission
            receiveStatusCode(tagID, worker, CONNECTION_TIMEOUT_MS);
        } else {
            sendStatusCode(tagID, "404", endpoint, worker, CONNECTION_TIMEOUT_MS);
        }
        log.info("Get operation completed");
    }

    private void delOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws TimeoutException, NullPointerException {
        final String keyToDelete = receiveKey(tagID, worker, CONNECTION_TIMEOUT_MS);
        final byte[] id = generateID(keyToDelete);

        String statusCode = "404";

        if (plasmaClient.contains(id)) {
            log.info("Entry with id {} exists", id);
            final PlasmaEntry entry = getPlasmaEntry(plasmaClient, id, PLASMA_TIMEOUT_MS);
            statusCode = findAndDeleteEntryWithKey(plasmaClient, keyToDelete, entry, id, new byte[20], PLASMA_TIMEOUT_MS);
        }

        if ("204".equals(statusCode)) {
            log.info("Object with key \"{}\" found and deleted", keyToDelete);
        } else {
            log.warn("Object with key \"{}\" was not found in plasma store", keyToDelete);
        }
        sendStatusCode(tagID, statusCode, endpoint, worker, CONNECTION_TIMEOUT_MS);
        log.info("Del operation completed");
    }

    private void regOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws TimeoutException {
        final InetSocketAddress newServerAddress = receiveAddress(tagID, worker, CONNECTION_TIMEOUT_MS);

        if (this.serverMap.containsValue(newServerAddress)) {
            sendStatusCode(tagID, "400", endpoint, worker, CONNECTION_TIMEOUT_MS);
            return;
        }

        if (this.serverID == 0) {
            log.info("This is the main server");
            final int currentServerCount = this.serverCount.incrementAndGet();
            sendServerMap(tagID, this.serverMap, worker, endpoint, currentServerCount, CONNECTION_TIMEOUT_MS);
            this.serverMap.put(currentServerCount - 1, newServerAddress);
        } else {
            log.info("This is a secondary server");
            sendStatusCode(tagID, "206", endpoint, worker, CONNECTION_TIMEOUT_MS);
            final int serverID = receiveInteger(tagID, worker, CONNECTION_TIMEOUT_MS);
            this.serverMap.put(serverID, newServerAddress);
            this.serverCount.incrementAndGet();
        }
    }

    private void infOperation(final int tagID, final Worker currentWorker, final Endpoint endpoint) throws TimeoutException {
        final int currentServerCount = this.serverCount.get();
        sendServerMap(tagID, this.serverMap, currentWorker, endpoint, currentServerCount, CONNECTION_TIMEOUT_MS);
        receiveStatusCode(tagID, currentWorker, CONNECTION_TIMEOUT_MS);
    }
}
