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
import org.apache.commons.lang3.SerializationException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import utils.DPwRErrorHandler;
import utils.WorkerPool;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static server.PlasmaServer.startPlasmaStore;
import static utils.CommunicationUtils.receiveAddress;
import static utils.CommunicationUtils.receiveInteger;
import static utils.CommunicationUtils.receiveStatusCode;
import static utils.CommunicationUtils.receiveTagID;
import static utils.CommunicationUtils.sendAddress;
import static utils.CommunicationUtils.sendOperationName;
import static utils.CommunicationUtils.sendSingleInteger;

@Slf4j
public class DPwRServer {

    public static final AtomicInteger serverCount = new AtomicInteger(1);
    private static final ErrorHandler errorHandler = new DPwRErrorHandler();
    private static final ContextParameters.Feature[] FEATURE_SET = {ContextParameters.Feature.TAG, ContextParameters.Feature.RMA, ContextParameters.Feature.WAKEUP};
    public static Map<Integer, InetSocketAddress> serverMap = new HashMap<>();
    public static int serverID = -1;
    private final InetSocketAddress listenAddress;
    private final int clientTimeout;
    private final int workerCount;
    private final ResourcePool resources = new ResourcePool();
    private Worker worker;
    private Context context;
    // listener parameter need to stay in memory, since it holds the callback for new connection requests
    @SuppressWarnings("FieldCanBeLocal")
    private ListenerParameters listenerParameters;
    private WorkerPool workerPool;

    public DPwRServer(final InetSocketAddress listenAddress, final int plasmaStoreSize, final int clientTimeout, final int workerCount, final Boolean verbose) {
        this.listenAddress = listenAddress;
        this.clientTimeout = clientTimeout;
        this.workerCount = workerCount;
        if (verbose) {
            setLogLevel(Level.INFO);
        } else {
            setLogLevel(Level.OFF);
        }
        startPlasmaStore(plasmaStoreSize);
        initPlasmaLibrary();
        serverID = 0;
        serverMap.put(0, this.listenAddress);
    }

    public DPwRServer(final InetSocketAddress listenAddress, final InetSocketAddress mainServerAddress, final int plasmaStoreSize, final int clientTimeout, final int workerCount, final Boolean verbose) throws ControlException, TimeoutException, ConnectException, SerializationException {
        this.listenAddress = listenAddress;
        this.clientTimeout = clientTimeout;
        this.workerCount = workerCount;
        if (verbose) {
            setLogLevel(Level.INFO);
        } else {
            setLogLevel(Level.WARN);
        }
        startPlasmaStore(plasmaStoreSize);
        initPlasmaLibrary();
        registerServer(mainServerAddress);
    }

    private void setLogLevel(final Level level) {
        final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        final org.apache.logging.log4j.core.config.Configuration config = ctx.getConfiguration();
        final LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        loggerConfig.setLevel(level);
        ctx.updateLoggers();
    }

    private void initPlasmaLibrary() {
        System.loadLibrary("plasma_java");
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

            for (final Map.Entry<Integer, InetSocketAddress> entry : serverMap.entrySet()) {
                final int serverID = entry.getKey();
                if (serverID == 0 || serverID == DPwRServer.serverID) {
                    continue;
                }
                final InetSocketAddress address = entry.getValue();
                registerAtSecondary(address, worker, scope);
            }
        } catch (final CloseException e) {
            log.error(e.getMessage());
        }

        log.info(String.valueOf(serverID));
        for (final var entry : serverMap.entrySet()) {
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
            DPwRServer.serverCount.set(serverCount);
            serverID = serverCount - 1;

            for (int i = 0; i < serverCount; i++) {
                final InetSocketAddress serverAddress = receiveAddress(tagID, worker, clientTimeout, scope);
                serverMap.put(i, serverAddress);
            }
            serverMap.put(serverID, this.listenAddress);
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
            case "206" -> sendSingleInteger(tagID, serverID, endpoint, worker, clientTimeout, scope);
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
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
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
        this.workerPool = new WorkerPool(this.workerCount, workerParameters);

        // Creating clean up hook
        final Thread cleanUpThread = new Thread(() -> {
            log.warn("Attempting graceful shutdown");
            try {
                PlasmaServer.cleanup();
                workerPool.close();
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
    private void listenLoop() throws ControlException, InterruptedException, TimeoutException {
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
                final WorkerThread currentWorkerThread = this.workerPool.getNextWorkerThread();
                currentWorkerThread.addClient(request);
            }
        }
    }
}
