package server;

import de.hhu.bsinfo.infinileap.binding.ConnectionHandler;
import de.hhu.bsinfo.infinileap.binding.ConnectionRequest;
import de.hhu.bsinfo.infinileap.binding.Context;
import de.hhu.bsinfo.infinileap.binding.ContextParameters;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.binding.Endpoint;
import de.hhu.bsinfo.infinileap.binding.EndpointParameters;
import de.hhu.bsinfo.infinileap.binding.ErrorHandler;
import de.hhu.bsinfo.infinileap.binding.ListenerParameters;
import de.hhu.bsinfo.infinileap.binding.NativeLogger;
import de.hhu.bsinfo.infinileap.binding.ThreadMode;
import de.hhu.bsinfo.infinileap.binding.Worker;
import de.hhu.bsinfo.infinileap.binding.WorkerParameters;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.Requests;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import utils.DPwRErrorHandler;
import utils.WorkerPool;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static server.PlasmaServer.startPlasmaStore;
import static utils.CommunicationUtils.awaitPutCompletionSignal;
import static utils.CommunicationUtils.createEntryAndSendNewEntryAddress;
import static utils.CommunicationUtils.receiveAddress;
import static utils.CommunicationUtils.receiveInteger;
import static utils.CommunicationUtils.receiveKey;
import static utils.CommunicationUtils.receiveOperationName;
import static utils.CommunicationUtils.receiveStatusCode;
import static utils.CommunicationUtils.receiveTagID;
import static utils.CommunicationUtils.sendAddress;
import static utils.CommunicationUtils.sendHash;
import static utils.CommunicationUtils.sendObjectAddress;
import static utils.CommunicationUtils.sendOperationName;
import static utils.CommunicationUtils.sendServerMap;
import static utils.CommunicationUtils.sendSingleInteger;
import static utils.CommunicationUtils.sendStatusCode;
import static utils.CommunicationUtils.tearDownEndpoint;
import static utils.HashUtils.generateID;
import static utils.HashUtils.generateNextIdOfId;
import static utils.PlasmaUtils.findAndDeleteEntryWithKey;
import static utils.PlasmaUtils.findEntryWithKey;
import static utils.PlasmaUtils.getObjectIdOfNextEntryWithEmptyNextID;
import static utils.PlasmaUtils.getPlasmaEntry;
import static utils.PlasmaUtils.getPlasmaEntryFromBuffer;

@Slf4j
public class DPwRServer {

    private static final ErrorHandler errorHandler = new DPwRErrorHandler();
    private static final ContextParameters.Feature[] FEATURE_SET = {ContextParameters.Feature.TAG, ContextParameters.Feature.RMA, ContextParameters.Feature.WAKEUP};
    private final InetSocketAddress listenAddress;
    private final int plasmaTimeout;
    private final int clientTimeout;
    private final int workerCount;
    private final AtomicInteger serverCount = new AtomicInteger(1);
    private final Map<Integer, InetSocketAddress> serverMap = new HashMap<>();
    private final ResourcePool resources = new ResourcePool();
    private final AtomicInteger runningTagID = new AtomicInteger(0);
    private PlasmaClient plasmaClient;
    private Worker worker;
    private Context context;
    private ExecutorService executorService;
    // listener parameter need to stay in memory, since it holds the callback for new connection requests
    @SuppressWarnings("FieldCanBeLocal")
    private ListenerParameters listenerParameters;
    private WorkerPool workerPool;
    private int serverID = -1;

    public DPwRServer(final InetSocketAddress listenAddress, final int plasmaStoreSize, final int plasmaTimeout, final int clientTimeout, final int workerCount, final Boolean verbose) {
        this.listenAddress = listenAddress;
        this.plasmaTimeout = plasmaTimeout;
        this.clientTimeout = clientTimeout;
        this.workerCount = workerCount;
        if (verbose) {
            setLogLevel(Level.INFO);
        } else {
            setLogLevel(Level.OFF);
        }
        startPlasmaStore(plasmaStoreSize);
        connectPlasma();
        this.serverID = 0;
        serverMap.put(0, this.listenAddress);
    }

    public DPwRServer(final InetSocketAddress listenAddress, final InetSocketAddress mainServerAddress, final int plasmaStoreSize, final int plasmaTimeout, final int clientTimeout, final int workerCount, final Boolean verbose) throws ControlException, TimeoutException, ConnectException, SerializationException {
        this.listenAddress = listenAddress;
        this.plasmaTimeout = plasmaTimeout;
        this.clientTimeout = clientTimeout;
        this.workerCount = workerCount;
        if (verbose) {
            setLogLevel(Level.INFO);
        } else {
            setLogLevel(Level.WARN);
        }
        startPlasmaStore(plasmaStoreSize);
        connectPlasma();
        registerServer(mainServerAddress);
    }

    private void setLogLevel(final Level level) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
        final LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(level);
        ctx.updateLoggers();
    }

    private void connectPlasma() {
        System.loadLibrary("plasma_java");
        try {
            this.plasmaClient = new PlasmaClient(PlasmaServer.getStoreAddress(), "", 0);
        } catch (final Exception e) {
            log.error("PlasmaDB could not be reached");
        }
    }

    private void registerServer(final InetSocketAddress mainServerAddress) throws ControlException, TimeoutException, ConnectException, SerializationException {
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

    private void registerAtMain(final InetSocketAddress mainServerAddress, final Worker worker, final ResourceScope scope) throws ControlException, TimeoutException, ConnectException, SerializationException {
        log.info("Register at main: {}", mainServerAddress);

        final EndpointParameters endpointParams = new EndpointParameters(scope).setRemoteAddress(mainServerAddress).setErrorHandler(errorHandler);
        final Endpoint endpoint = worker.createEndpoint(endpointParams);

        final int tagID = receiveTagID(worker, clientTimeout, scope);
        sendOperationName(tagID, "REG", endpoint, worker, clientTimeout, scope);
        sendAddress(tagID, this.listenAddress, endpoint, worker, clientTimeout);

        final String statusCode = receiveStatusCode(tagID, worker, clientTimeout, scope);

        if ("200".equals(statusCode)) {
            final int serverCount = receiveInteger(tagID, worker, clientTimeout, scope);
            this.serverCount.set(serverCount);
            this.serverID = serverCount - 1;

            for (int i = 0; i < serverCount; i++) {
                final InetSocketAddress serverAddress = receiveAddress(tagID, worker, clientTimeout, scope);
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

        final int tagID = receiveTagID(worker, clientTimeout, scope);
        sendOperationName(tagID, "REG", endpoint, worker, clientTimeout, scope);
        sendAddress(tagID, this.listenAddress, endpoint, worker, clientTimeout);

        final String statusCode = receiveStatusCode(tagID, worker, clientTimeout, scope);

        switch (statusCode) {
            case "206" -> sendSingleInteger(tagID, this.serverID, endpoint, worker, clientTimeout, scope);
            case "200" ->
                    throw new ConnectException("Given address was of the main server and not of the secondary server");
            case "400" -> throw new ConnectException("This server was already registered");
            default -> throw new ConnectException("Secondary server could not be reached");
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
        this.workerPool = new WorkerPool(this.workerCount, workerParameters, context);
        this.executorService = Executors.newCachedThreadPool();

        // Creating clean up hook
        final Thread cleanUpThread = new Thread(() -> {
            log.warn("Attempting graceful shutdown");
            try {
                PlasmaServer.cleanup();
                log.info("Closing Resources");
                resources.close();
            } catch (final Exception e) {
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
        this.listenerParameters = new ListenerParameters().setListenAddress(listenAddress).setConnectionHandler(new ConnectionHandler() {
            @Override
            protected void onConnection(final ConnectionRequest request) {
                connectionQueue.add(request);
            }
        });
        pushResource(this.worker.createListener(this.listenerParameters));

        while (true) {
            Requests.await(this.worker, connectionQueue);
            while (!connectionQueue.isEmpty()) {
                final ConnectionRequest request = connectionQueue.remove();
                final Worker currentWorker = this.workerPool.getNextWorker();
                executorService.submit(() -> {
                    Endpoint currentEndpoint = null;
                    try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
                        final int currentTagID = this.runningTagID.getAndIncrement();
                        currentEndpoint = establishConnection(request, currentWorker, currentTagID, scope);
                        handleRequestLoop(currentWorker, currentEndpoint, currentTagID);
                    } catch (final Exception e) {
                        log.error(e.getMessage());
                    } finally {
                        if (ObjectUtils.isNotEmpty(currentEndpoint)) {
                            tearDownEndpoint(currentEndpoint, currentWorker, clientTimeout);
                        }
                    }
                });
            }
        }
    }

    private Endpoint establishConnection(final ConnectionRequest request, final Worker currentWorker, final int currentTagID, final ResourceScope scope) throws ControlException, TimeoutException {
        log.info("Establish connection");
        final Endpoint endpoint = currentWorker.createEndpoint(new EndpointParameters(scope).setConnectionRequest(request).setErrorHandler(errorHandler));
        sendSingleInteger(0, currentTagID, endpoint, currentWorker, clientTimeout, scope);
        return endpoint;
    }

    private void handleRequestLoop(final Worker worker, final Endpoint endpoint, final int tagID) throws ControlException, CloseException, TimeoutException, IOException, ClassNotFoundException {
        handleLoop:
        while (true) {
            worker.await();
            String operationName;
            try {
                operationName = receiveOperationName(tagID, worker, clientTimeout);
            } catch (final Exception e) {
                log.error(e.getMessage());
                operationName = "";
            }
            switch (operationName) {
                case "PUT" -> putOperation(tagID, worker, endpoint);
                case "GET" -> getOperation(tagID, worker, endpoint);
                case "DEL" -> deleteOperation(tagID, worker, endpoint);
                case "CNT" -> containsOperation(tagID, worker, endpoint);
                case "HSH" -> hashOperation(tagID, worker, endpoint);
                case "LST" -> listOperation(tagID, worker, endpoint);
                case "REG" -> regOperation(tagID, worker, endpoint);
                case "INF" -> infOperation(tagID, worker, endpoint);
                case "BYE" -> {
                    sendStatusCode(tagID, "BYE", endpoint, worker, clientTimeout, ResourceScope.newConfinedScope());
                    break handleLoop;
                }
            }
        }
    }

    private void putOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws TimeoutException, ControlException, CloseException, IOException, ClassNotFoundException {
        log.info("Start PUT operation");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final String keyToPut = receiveKey(tagID, worker, clientTimeout, scope);
            byte[] id = generateID(keyToPut);
            final int entrySize = receiveInteger(tagID, worker, clientTimeout, scope);
            final String statusCode;

            if (plasmaClient.contains(id)) {
                log.warn("Plasma does contain the id");
                final PlasmaEntry plasmaEntry = getPlasmaEntry(plasmaClient, id, plasmaTimeout);
                final byte[] objectIdWithFreeNextID = getObjectIdOfNextEntryWithEmptyNextID(plasmaClient, plasmaEntry, id, keyToPut, plasmaTimeout);

                if (ArrayUtils.isEmpty(objectIdWithFreeNextID)) {
                    log.warn("Object with key is already in plasma");
                    statusCode = "400";
                } else {
                    log.warn("Key is not in plasma, handling id collision");
                    id = generateNextIdOfId(objectIdWithFreeNextID);

                    createEntryAndSendNewEntryAddress(tagID, plasmaClient, id, entrySize, endpoint, worker, context, clientTimeout, scope);
                    statusCode = awaitPutCompletionSignal(tagID, plasmaClient, id, worker, objectIdWithFreeNextID, clientTimeout, plasmaTimeout, scope);
                }
            } else {
                log.info("Plasma does not contain the id");
                createEntryAndSendNewEntryAddress(tagID, plasmaClient, id, entrySize, endpoint, worker, context, clientTimeout, scope);
                statusCode = awaitPutCompletionSignal(tagID, plasmaClient, id, worker, null, clientTimeout, plasmaTimeout, scope);
            }
            sendStatusCode(tagID, statusCode, endpoint, worker, clientTimeout, scope);
        }
        log.info("PUT operation completed");
    }

    private void getOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws ControlException, TimeoutException, CloseException, IOException, ClassNotFoundException {
        log.info("Start GET operation");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final String keyToGet = receiveKey(tagID, worker, clientTimeout, scope);
            final byte[] id = generateID(keyToGet);

            ByteBuffer entryBuffer = null;

            if (plasmaClient.contains(id)) {
                entryBuffer = plasmaClient.getObjAsByteBuffer(id, plasmaTimeout, false);
                final PlasmaEntry entry = getPlasmaEntryFromBuffer(entryBuffer);

                if (!StringUtils.equals(keyToGet, entry.key)) {
                    log.warn("Entry with id: {} has not key: {}", id, keyToGet);
                    entryBuffer = findEntryWithKey(plasmaClient, keyToGet, entryBuffer, plasmaTimeout);
                }
            }

            if (ObjectUtils.isNotEmpty(entryBuffer)) {
                sendStatusCode(tagID, "211", endpoint, worker, clientTimeout, scope);
                sendObjectAddress(tagID, entryBuffer, endpoint, worker, context, clientTimeout);
                // Wait for client to signal successful transmission
                final String statusCode;
                statusCode = receiveStatusCode(tagID, worker, clientTimeout, scope);
                if ("212".equals(statusCode)) {
                    sendStatusCode(tagID, "213", endpoint, worker, clientTimeout, scope);
                } else {
                    sendStatusCode(tagID, "412", endpoint, worker, clientTimeout, scope);
                }
            } else {
                sendStatusCode(tagID, "411", endpoint, worker, clientTimeout, scope);
            }
        }
        log.info("GET operation completed");
    }

    private void deleteOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws TimeoutException, NullPointerException, IOException, ClassNotFoundException {
        log.info("Start DEL operation");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final String keyToDelete = receiveKey(tagID, worker, clientTimeout, scope);
            final byte[] id = generateID(keyToDelete);

            String statusCode = "421";

            if (plasmaClient.contains(id)) {
                log.info("Entry with id {} exists", id);
                final PlasmaEntry entry = getPlasmaEntry(plasmaClient, id, plasmaTimeout);
                statusCode = findAndDeleteEntryWithKey(plasmaClient, keyToDelete, entry, id, new byte[20], plasmaTimeout);
            }

            if ("221".equals(statusCode)) {
                log.info("Object with key \"{}\" found and deleted", keyToDelete);
            } else {
                log.warn("Object with key \"{}\" was not found in plasma store", keyToDelete);
            }
            sendStatusCode(tagID, statusCode, endpoint, worker, clientTimeout, scope);
        }
        log.info("DEL operation completed");
    }

    private void containsOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws TimeoutException {
        log.info("Start CNT operation");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final String keyToGet = receiveKey(tagID, worker, clientTimeout, scope);
            final byte[] id = generateID(keyToGet);

            if (plasmaClient.contains(id)) {
                sendStatusCode(tagID, "231", endpoint, worker, clientTimeout, scope);
            } else {
                sendStatusCode(tagID, "431", endpoint, worker, clientTimeout, scope);
            }
        }
        log.info("CNT operation completed");
    }

    private void hashOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws TimeoutException {
        log.info("Start HSH operation");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final String keyToGet = receiveKey(tagID, worker, clientTimeout, scope);
            final byte[] id = generateID(keyToGet);

            final byte[] hash = plasmaClient.hash(id);

            if (ObjectUtils.isEmpty(hash)) {
                sendStatusCode(tagID, "441", endpoint, worker, clientTimeout, scope);
            } else {
                sendStatusCode(tagID, "241", endpoint, worker, clientTimeout, scope);
                sendHash(tagID, hash, endpoint, worker, clientTimeout);
            }
            sendStatusCode(tagID, "242", endpoint, worker, clientTimeout, scope);
        }
        log.info("HSH operation completed");
    }

    private void listOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws ControlException, TimeoutException, CloseException {
        log.info("Start LST operation");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final List<byte[]> entries = plasmaClient.list();
            sendSingleInteger(tagID, entries.size(), endpoint, worker, clientTimeout, scope);
            for (final byte[] entry : entries) {
                sendObjectAddress(tagID, entry, endpoint, worker, context, clientTimeout);
                // Wait for client to signal successful transmission
                receiveStatusCode(tagID, worker, clientTimeout, scope);
            }
        }
        log.info("LST operation completed");
    }

    private void regOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws TimeoutException {
        log.info("Start REG operation");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final InetSocketAddress newServerAddress = receiveAddress(tagID, worker, clientTimeout, scope);

            if (this.serverMap.containsValue(newServerAddress)) {
                sendStatusCode(tagID, "400", endpoint, worker, clientTimeout, scope);
                return;
            }

            if (this.serverID == 0) {
                log.info("This is the main server");
                final int currentServerCount = this.serverCount.incrementAndGet();
                sendStatusCode(tagID, "200", endpoint, worker, clientTimeout, scope);
                sendServerMap(tagID, this.serverMap, worker, endpoint, currentServerCount, clientTimeout);
                this.serverMap.put(currentServerCount - 1, newServerAddress);
            } else {
                log.info("This is a secondary server");
                sendStatusCode(tagID, "206", endpoint, worker, clientTimeout, scope);
                final int serverID = receiveInteger(tagID, worker, clientTimeout, scope);
                this.serverMap.put(serverID, newServerAddress);
                this.serverCount.incrementAndGet();
            }
        }
        log.info(serverMap.entrySet().toString());
        log.info("REG operation completed");
    }

    private void infOperation(final int tagID, final Worker currentWorker, final Endpoint endpoint) throws TimeoutException {
        log.info("Start INF operation");
        final int currentServerCount = this.serverCount.get();
        sendServerMap(tagID, this.serverMap, currentWorker, endpoint, currentServerCount, clientTimeout);
        log.info("INF operation completed");
    }
}
