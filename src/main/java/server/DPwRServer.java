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
import org.apache.commons.lang3.SerializationUtils;
import utils.WorkerPool;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;
import static utils.CommunicationUtils.*;
import static utils.HashUtils.generateID;
import static utils.HashUtils.generateNextIdOfId;
import static utils.PlasmaUtils.*;

@Slf4j
public class DPwRServer {

    private static final int OPERATION_MESSAGE_SIZE = 10;

    int serverID = -1;
    private final AtomicInteger serverCount = new AtomicInteger(1);
    private final Map<Integer, InetSocketAddress> serverMap = new HashMap<>();

    private PlasmaClient plasmaClient;
    private final String plasmaFilePath;

    private final ResourcePool resources = new ResourcePool();
    private static final ContextParameters.Feature[] FEATURE_SET = {ContextParameters.Feature.TAG, ContextParameters.Feature.RMA, ContextParameters.Feature.WAKEUP};
    private static final int CONNECTION_TIMEOUT_MS = 750;
    private static final int PLASMA_TIMEOUT_MS = 500;
    private Worker worker;
    private Context context;
    private final InetSocketAddress listenAddress;
    private final SafeCounterWithoutLock runningTagID = new SafeCounterWithoutLock();
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

        final EndpointParameters endpointParams = new EndpointParameters(scope).setRemoteAddress(mainServerAddress).setPeerErrorHandlingMode();
        final Endpoint endpoint = worker.createEndpoint(endpointParams);

        final int tagID = receiveTagID(worker, CONNECTION_TIMEOUT_MS);
        sendSingleMessage(tagID, SerializationUtils.serialize("REG"), endpoint, worker, CONNECTION_TIMEOUT_MS);

        final byte[] addressBytes = SerializationUtils.serialize(this.listenAddress);
        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(addressBytes.length);
        sendSingleMessage(tagID, byteBuffer.array(), endpoint, worker, CONNECTION_TIMEOUT_MS);
        sendSingleMessage(tagID, addressBytes, endpoint, worker, CONNECTION_TIMEOUT_MS);

        final String statusCode = receiveStatusCode(tagID, worker, CONNECTION_TIMEOUT_MS);

        if ("200".equals(statusCode)) {
            final int serverCount = receiveInteger(tagID, worker, CONNECTION_TIMEOUT_MS);
            this.serverCount.set(serverCount);
            this.serverID = serverCount - 1;

            for (int i = 0; i < serverCount; i++) {
                final int addressSize = receiveInteger(tagID, worker, CONNECTION_TIMEOUT_MS);
                final byte[] serverAddressBytes = receiveData(tagID, addressSize, worker, CONNECTION_TIMEOUT_MS);
                final InetSocketAddress inetSocketAddress = deserialize(serverAddressBytes);
                serverMap.put(i, inetSocketAddress);
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

        final EndpointParameters endpointParams = new EndpointParameters(scope).setRemoteAddress(address).setPeerErrorHandlingMode();
        final Endpoint endpoint = worker.createEndpoint(endpointParams);

        final int tagID = receiveTagID(worker, CONNECTION_TIMEOUT_MS);
        sendSingleMessage(tagID, SerializationUtils.serialize("REG"), endpoint, worker, CONNECTION_TIMEOUT_MS);

        final byte[] addressBytes = SerializationUtils.serialize(this.listenAddress);
        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(addressBytes.length);
        sendSingleMessage(tagID, byteBuffer.array(), endpoint, worker, CONNECTION_TIMEOUT_MS);
        sendSingleMessage(tagID, addressBytes, endpoint, worker, CONNECTION_TIMEOUT_MS);

        final String statusCode = receiveStatusCode(tagID, worker, CONNECTION_TIMEOUT_MS);

        if ("206".equals(statusCode)) {
            sendSingleMessage(tagID, ByteBuffer.allocate(Integer.BYTES).putInt(this.serverID).array(), endpoint, worker, CONNECTION_TIMEOUT_MS);
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

    private void initialize() throws ControlException, InterruptedException {
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

        this.workerPool = new WorkerPool(6, workerParameters, context);
        this.executorService = Executors.newFixedThreadPool(2);

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
    private void listenLoop() throws InterruptedException, ControlException {
        final var connectionQueue = new LinkedBlockingQueue<ConnectionRequest>();

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
                    } catch (final ControlException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }

    private void handleRequest(final ConnectionRequest request, final Worker currentWorker) throws ControlException {
        try (final ResourceScope scope = ResourceScope.newConfinedScope(); final Endpoint endpoint = currentWorker.createEndpoint(new EndpointParameters(scope).setConnectionRequest(request).setPeerErrorHandlingMode())) {
            // Send tagID to client
            final int tagID = this.runningTagID.getValue();
            this.runningTagID.increment();
            final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(tagID);
            sendSingleMessage(0, byteBuffer.array(), endpoint, currentWorker, CONNECTION_TIMEOUT_MS);

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
                case "REG" -> {
                    log.info("Start REG operation");
                    regOperation(tagID, currentWorker, endpoint);
                }
            }
        } catch (final NullPointerException | CloseException | TimeoutException | ExecutionException | ControlException e) {
            log.error(e.getMessage());
        }
    }

    private void putOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws TimeoutException, ControlException, CloseException {
        final String keyToPut = receiveKey(tagID, worker, CONNECTION_TIMEOUT_MS);
        byte[] id = generateID(keyToPut);
        final int entrySize = receiveInteger(tagID, worker, CONNECTION_TIMEOUT_MS);

        if (plasmaClient.contains(id)) {
            log.warn("Plasma does contain the id");
            final PlasmaEntry plasmaEntry = deserialize(plasmaClient.get(id, PLASMA_TIMEOUT_MS, false));
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

        if (plasmaClient.contains(id)) {
            final ByteBuffer objectBuffer = plasmaClient.getObjAsByteBuffer(id, PLASMA_TIMEOUT_MS, false);
            final PlasmaEntry entry = getPlasmaEntryFromBuffer(objectBuffer);

            if (keyToGet.equals(entry.key)) {
                log.info("Entry with id: {} has key: {}", id, keyToGet);

                sendObjectAddressAndStatusCode(tagID, objectBuffer, endpoint, worker, context, CONNECTION_TIMEOUT_MS);

                // Wait for client to signal successful transmission
                receiveStatusCode(tagID, worker, CONNECTION_TIMEOUT_MS);
            } else {
                log.warn("Entry with id: {} has not key: {}", id, keyToGet);
                final ByteBuffer bufferOfCorrectEntry = findEntryWithKey(plasmaClient, keyToGet, objectBuffer, PLASMA_TIMEOUT_MS);

                if (bufferOfCorrectEntry != null) {
                    log.info("Found entry with key: {}", keyToGet);

                    sendObjectAddressAndStatusCode(tagID, bufferOfCorrectEntry, endpoint, worker, context, CONNECTION_TIMEOUT_MS);

                    // Wait for client to signal successful transmission
                    receiveStatusCode(tagID, worker, CONNECTION_TIMEOUT_MS);
                } else {
                    log.warn("Not found entry with key: {}", keyToGet);
                    sendSingleMessage(tagID, serialize("404"), endpoint, worker, CONNECTION_TIMEOUT_MS);
                }
            }
        } else {
            log.warn("Not found entry with key: {}", keyToGet);
            sendSingleMessage(tagID, serialize("404"), endpoint, worker, CONNECTION_TIMEOUT_MS);
        }
        log.info("Get operation completed \n");
    }

    private void delOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws ExecutionException, TimeoutException, NullPointerException {
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

    private void regOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws TimeoutException {
        // Receive the server address from the new server
        final int newAddressSize = receiveInteger(tagID, worker, CONNECTION_TIMEOUT_MS);
        final byte[] newAddressBytes = receiveData(tagID, newAddressSize, worker, CONNECTION_TIMEOUT_MS);
        final InetSocketAddress newServerAddress = SerializationUtils.deserialize(newAddressBytes);

        if (this.serverMap.containsValue(newServerAddress)) {
            sendSingleMessage(tagID, serialize("400"), endpoint, worker, CONNECTION_TIMEOUT_MS);
            return;
        }

        if (this.serverID == 0) {
            log.info("This is the main server");
            try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
                final ArrayList<Long> requests = new ArrayList<>();
                requests.add(prepareToSendString(tagID, "200", endpoint, scope));
                // Send the current server count
                final int currentServerCount = this.serverCount.incrementAndGet();
                requests.add(prepareToSendInteger(tagID, currentServerCount, endpoint, scope));
                // For each server address in the server map first send its size, then the address
                for (int i = 0; i < currentServerCount; i++) {
                    requests.addAll(prepareToSendAddress(tagID, serverMap.get(i), endpoint, scope));
                }
                sendData(requests, worker, CONNECTION_TIMEOUT_MS);
                this.serverMap.put(currentServerCount - 1, newServerAddress);
            }
        } else {
            log.info("This is a secondary server");
            sendSingleMessage(tagID, SerializationUtils.serialize("206"), endpoint, worker, CONNECTION_TIMEOUT_MS);
            final byte[] serverIDBytes = receiveData(tagID, Integer.BYTES, worker, CONNECTION_TIMEOUT_MS);
            final int serverID = ByteBuffer.wrap(serverIDBytes).getInt();

            this.serverMap.put(serverID, newServerAddress);
            this.serverCount.incrementAndGet();
        }
        log.info(String.valueOf(this.serverID));
        for (final var entry : this.serverMap.entrySet()) {
            log.info(entry.getKey() + "/" + entry.getValue());
        }
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