package server;

import de.hhu.bsinfo.infinileap.binding.ConnectionRequest;
import de.hhu.bsinfo.infinileap.binding.Context;
import de.hhu.bsinfo.infinileap.binding.ContextParameters;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.binding.Endpoint;
import de.hhu.bsinfo.infinileap.binding.EndpointParameters;
import de.hhu.bsinfo.infinileap.binding.ErrorHandler;
import de.hhu.bsinfo.infinileap.binding.NativeLogger;
import de.hhu.bsinfo.infinileap.binding.RequestParameters;
import de.hhu.bsinfo.infinileap.binding.Worker;
import de.hhu.bsinfo.infinileap.binding.WorkerParameters;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;
import model.PlasmaEntry;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import utils.DPwRErrorHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static server.PlasmaServer.plasmaClient;
import static utils.CommunicationUtils.awaitPutCompletionSignal;
import static utils.CommunicationUtils.awaitRequests;
import static utils.CommunicationUtils.createEntryAndSendNewEntryAddress;
import static utils.CommunicationUtils.receiveAddress;
import static utils.CommunicationUtils.receiveInteger;
import static utils.CommunicationUtils.receiveKey;
import static utils.CommunicationUtils.receiveOperationName;
import static utils.CommunicationUtils.receiveStatusCode;
import static utils.CommunicationUtils.receiveTagIDAsStream;
import static utils.CommunicationUtils.sendHash;
import static utils.CommunicationUtils.sendObjectAddress;
import static utils.CommunicationUtils.sendServerMap;
import static utils.CommunicationUtils.sendSingleInteger;
import static utils.CommunicationUtils.sendStatusCode;
import static utils.CommunicationUtils.streamTagID;
import static utils.HashUtils.generateID;
import static utils.HashUtils.generateNextIdOfId;
import static utils.PlasmaUtils.findAndDeleteEntryWithKey;
import static utils.PlasmaUtils.findEntryWithKey;
import static utils.PlasmaUtils.getObjectIdOfNextEntryWithEmptyNextID;
import static utils.PlasmaUtils.getPlasmaEntry;
import static utils.PlasmaUtils.getPlasmaEntryFromBuffer;

@Slf4j
public class WorkerThread extends Thread {
    private static final ErrorHandler errorHandler = new DPwRErrorHandler();
    private static final ContextParameters.Feature[] FEATURE_SET = {ContextParameters.Feature.TAG, ContextParameters.Feature.RMA, ContextParameters.Feature.WAKEUP, ContextParameters.Feature.STREAM};
    public final Worker worker;
    private final ResourcePool resources = new ResourcePool();
    private final Context context;
    private final List<Pair<Endpoint, Integer>> endpointsAndTags = new ArrayList<>();
    private final AtomicInteger runningTagID = new AtomicInteger(0);
    private final int clientTimeout = 500;
    private final int plasmaTimeout = 500;
    public BlockingQueue<ConnectionRequest> connectionRequests = new LinkedBlockingQueue<>();
    private boolean shutdown = false;


    public WorkerThread(final WorkerParameters workerParameters) throws ControlException {
        NativeLogger.enable();
        log.info("Using UCX version {}", Context.getVersion());

        // Initialize UCP context
        log.info("Initializing context");
        final ContextParameters contextParameters = new ContextParameters().setFeatures(FEATURE_SET);
        this.context = pushResource(Context.initialize(contextParameters, null));

        // Create a worker
        log.info("Creating worker");
        this.worker = pushResource(context.createWorker(workerParameters));
    }

    public void addClient(final ConnectionRequest request) {
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final int tagID = runningTagID.incrementAndGet();
            final Endpoint endpoint;
            try {
                endpoint = establishConnection(request, scope);
            } catch (final ControlException e) {
                log.error(e.getMessage());
                return;
            }
            try {
                sendFirstTagID(tagID, endpoint, scope);
            } catch (final TimeoutException e) {
                closeEndpoint(endpoint);
            }
            endpointsAndTags.add(Pair.of(endpoint, tagID));
            log.info("Full Map: {}", endpointsAndTags);
        }
    }

    private Endpoint establishConnection(final ConnectionRequest request, final ResourceScope scope) throws ControlException {
        return worker.createEndpoint(new EndpointParameters(scope)
                .setConnectionRequest(request)
                .setErrorHandler(errorHandler)
                .enableClientIdentifier());
    }

    private void sendFirstTagID(final int tagID, final Endpoint endpoint, final ResourceScope scope) throws TimeoutException {
        streamTagID(tagID, endpoint, worker, clientTimeout, scope);
    }

    @Override
    public void run() {
        while (!shutdown) {
            worker.await();
            ConnectionRequest request;
            while ((request = connectionRequests.poll()) != null) {
                addClient(request);
            }
            if (!endpointsAndTags.isEmpty()) {
                final ArrayList<Integer> toBeRemoved = new ArrayList<>();
                String operationName;
                for (int i = 0; i < endpointsAndTags.size(); i++) {
                    final Pair<Endpoint, Integer> pair = endpointsAndTags.get(i);
                    final Endpoint endpoint = pair.getLeft();
                    final int tagID = pair.getRight();
                    log.info("Current TagID: [{}]", tagID);

                    try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
                        final int currentTagID = receiveTagIDAsStream(endpoint, worker, clientTimeout, scope);
                        if (currentTagID != tagID) {
                            sendSingleInteger(tagID, 0, endpoint, worker, clientTimeout, scope);
                        } else {
                            final int newTagID = runningTagID.incrementAndGet();
                            streamTagID(newTagID, endpoint, worker, clientTimeout, scope);
                            endpointsAndTags.set(i, Pair.of(endpoint, newTagID));
                            operationName = receiveOperationName(newTagID, worker, clientTimeout, scope);
                            switch (operationName) {
                                case "PUT" -> putOperation(newTagID, worker, endpoint);
                                case "GET" -> getOperation(newTagID, worker, endpoint);
                                case "DEL" -> deleteOperation(newTagID, worker, endpoint);
                                case "CNT" -> containsOperation(newTagID, worker, endpoint);
                                case "HSH" -> hashOperation(newTagID, worker, endpoint);
                                case "LST" -> listOperation(newTagID, worker, endpoint);
                                case "REG" -> regOperation(newTagID, worker, endpoint);
                                case "INF" -> infOperation(newTagID, worker, endpoint);
                                case "BYE" -> {
                                    closeEndpoint(endpoint);
                                    toBeRemoved.add(i);
                                }
                            }
                        }
                    } catch (final ClassNotFoundException | TimeoutException | ControlException | CloseException |
                                   IOException e) {
                        log.error(e.getMessage());
                        closeEndpoint(endpoint);
                        toBeRemoved.add(i);
                    }
                }
                // Sorts descending such that we delete higher indices first
                toBeRemoved.sort(Comparator.reverseOrder());
                for (final int index : toBeRemoved) {
                    endpointsAndTags.remove(index);
                }
            }
        }
        try {
            resources.close();
        } catch (final CloseException e) {
            log.error(e.getMessage());
        }
    }

    private void putOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws TimeoutException, ControlException, CloseException, IOException, ClassNotFoundException {
        log.info("[{}] Start PUT operation", tagID);
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final String keyToPut = receiveKey(tagID, worker, clientTimeout, scope);
            byte[] id = generateID(keyToPut);
            final int entrySize = receiveInteger(tagID, worker, clientTimeout, scope);
            final String statusCode;

            if (plasmaClient.contains(id)) {
                log.warn("[{}] Plasma does contain the id", tagID);
                final PlasmaEntry plasmaEntry = getPlasmaEntry(plasmaClient, id, plasmaTimeout);
                final byte[] objectIdWithFreeNextID = getObjectIdOfNextEntryWithEmptyNextID(plasmaClient, plasmaEntry, id, keyToPut, plasmaTimeout);

                if (ArrayUtils.isEmpty(objectIdWithFreeNextID)) {
                    log.warn("[{}] Object with key is already in plasma", tagID);
                    statusCode = "400";
                } else {
                    log.warn("[{}] Key is not in plasma, handling id collision", tagID);
                    id = generateNextIdOfId(objectIdWithFreeNextID);

                    createEntryAndSendNewEntryAddress(tagID, plasmaClient, id, entrySize, endpoint, worker, context, clientTimeout, scope);
                    statusCode = awaitPutCompletionSignal(tagID, plasmaClient, id, worker, objectIdWithFreeNextID, clientTimeout, plasmaTimeout, scope);
                }
            } else {
                log.info("[{}] Plasma does not contain the id", tagID);
                createEntryAndSendNewEntryAddress(tagID, plasmaClient, id, entrySize, endpoint, worker, context, clientTimeout, scope);
                statusCode = awaitPutCompletionSignal(tagID, plasmaClient, id, worker, null, clientTimeout, plasmaTimeout, scope);
            }
            sendStatusCode(tagID, statusCode, endpoint, worker, clientTimeout, scope);
        }
        log.info("[{}] PUT operation completed", tagID);
    }

    private void getOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws ControlException, TimeoutException, CloseException, IOException, ClassNotFoundException {
        log.info("[{}] Start GET operation", tagID);
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final String keyToGet = receiveKey(tagID, worker, clientTimeout, scope);
            final byte[] id = generateID(keyToGet);

            ByteBuffer entryBuffer = null;

            if (plasmaClient.contains(id)) {
                entryBuffer = plasmaClient.getObjAsByteBuffer(id, plasmaTimeout, false);
                final PlasmaEntry entry = getPlasmaEntryFromBuffer(entryBuffer);

                if (!StringUtils.equals(keyToGet, entry.key)) {
                    log.warn("[{}] Entry with id: {} has not key: {}", tagID, id, keyToGet);
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
        log.info("[{}] GET operation completed", tagID);
    }

    private void deleteOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws TimeoutException, NullPointerException, IOException, ClassNotFoundException {
        log.info("[{}] Start DEL operation", tagID);
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final String keyToDelete = receiveKey(tagID, worker, clientTimeout, scope);
            final byte[] id = generateID(keyToDelete);

            String statusCode = "421";

            if (plasmaClient.contains(id)) {
                log.info("[{}] Entry with id {} exists", tagID, id);
                final PlasmaEntry entry = getPlasmaEntry(plasmaClient, id, plasmaTimeout);
                statusCode = findAndDeleteEntryWithKey(plasmaClient, keyToDelete, entry, id, new byte[20], plasmaTimeout);
            }

            if ("221".equals(statusCode)) {
                log.info("[{}] Object with key \"{}\" found and deleted", tagID, keyToDelete);
            } else {
                log.warn("[{}] Object with key \"{}\" was not found in plasma store", tagID, keyToDelete);
            }
            sendStatusCode(tagID, statusCode, endpoint, worker, clientTimeout, scope);
        }
        log.info("[{}] DEL operation completed", tagID);
    }

    private void containsOperation(final int tagID, final Worker worker, final Endpoint endpoint) throws TimeoutException {
        log.info("[{}] Start CNT operation", tagID);
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final String keyToGet = receiveKey(tagID, worker, clientTimeout, scope);
            final byte[] id = generateID(keyToGet);

            if (plasmaClient.contains(id)) {
                sendStatusCode(tagID, "231", endpoint, worker, clientTimeout, scope);
            } else {
                sendStatusCode(tagID, "431", endpoint, worker, clientTimeout, scope);
            }
        }
        log.info("[{}] CNT operation completed", tagID);
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
        log.info("[{}] Start LST operation", tagID);
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

            if (DPwRServer.serverMap.containsValue(newServerAddress)) {
                sendStatusCode(tagID, "400", endpoint, worker, clientTimeout, scope);
                return;
            }

            if (DPwRServer.serverID == 0) {
                log.info("This is the main server");
                final int currentServerCount = DPwRServer.serverCount.incrementAndGet();
                sendStatusCode(tagID, "200", endpoint, worker, clientTimeout, scope);
                sendServerMap(tagID, DPwRServer.serverMap, worker, endpoint, currentServerCount, clientTimeout);
                DPwRServer.serverMap.put(currentServerCount - 1, newServerAddress);
            } else {
                log.info("This is a secondary server");
                sendStatusCode(tagID, "206", endpoint, worker, clientTimeout, scope);
                final int serverID = receiveInteger(tagID, worker, clientTimeout, scope);
                DPwRServer.serverMap.put(serverID, newServerAddress);
                DPwRServer.serverCount.incrementAndGet();
            }
        }
        log.info(DPwRServer.serverMap.entrySet().toString());
        log.info("REG operation completed");
    }

    private void infOperation(final int tagID, final Worker currentWorker, final Endpoint endpoint) throws TimeoutException {
        log.info("Start INF operation");
        final int currentServerCount = DPwRServer.serverCount.get();
        sendServerMap(tagID, DPwRServer.serverMap, currentWorker, endpoint, currentServerCount, clientTimeout);
        log.info("INF operation completed");
    }

    private void closeEndpoint(final Endpoint endpoint) {
        try {
            final long[] request = new long[]{endpoint.closeNonBlocking(new RequestParameters().setFlags(RequestParameters.Flag.CLOSE_FORCE))};
            try {
                awaitRequests(request, worker, clientTimeout);
            } catch (final TimeoutException e) {
                endpoint.close();
            }
        } catch (final IndexOutOfBoundsException | IllegalStateException e) {
            log.error(e.getMessage());
        }
    }

    protected <T extends AutoCloseable> T pushResource(final T resource) {
        resources.push(resource);
        return resource;
    }

    public void close() {
        this.shutdown = true;
    }
}
