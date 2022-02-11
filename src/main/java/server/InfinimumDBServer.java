package server;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;
import org.apache.arrow.plasma.exceptions.PlasmaOutOfMemoryException;
import org.apache.commons.lang3.ArrayUtils;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;
import static server.CommunicationUtils.*;

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

    private void getOperation(Worker worker, Endpoint endpoint) throws ControlException {
        final byte[] dataSizeAsBytes = receiveData(Integer.BYTES, 0, worker, scope);
        ByteBuffer byteBuffer = ByteBuffer.wrap(dataSizeAsBytes);
        int dataSize = byteBuffer.getInt();
        if (log.isInfoEnabled()) {
            log.info("Received \"{}\"", dataSize);
        }
        final byte[] dataAsBytes = receiveData(dataSize, 0L, worker, scope);
        HashMap<String, String> data = deserialize(dataAsBytes);
        if (log.isInfoEnabled()) {
            log.info("Received \"{}\"", data);
        }

        byte[] id = new byte[0];
        try {
            id = getMD5Hash(data.get("key"));
        } catch (NoSuchAlgorithmException e) {
            if (log.isErrorEnabled()) {
                log.error("The MD5 hash algorithm was not found.", e);
            }
        }
        final byte[] fullID = ArrayUtils.addAll(id, new byte[4]);

        if (log.isInfoEnabled()) {
            log.info("Getting object from plasma store");
        }
        byte[] objectBytes = plasmaClient.get(fullID, 1, false);

        if (objectBytes != null && objectBytes.length > 0) {
            final MemoryDescriptor objectAddress;
            try {
                objectAddress = getMemoryDescriptorOfBytes(objectBytes, this.context);
            } catch (ControlException e) {
                if (log.isErrorEnabled()) {
                    log.error("An exception occurred getting the objects memory address, aborting GET operation");
                }
                sendSingleMessage(serialize("500"), 0L, endpoint, scope, worker);
                throw e;
            }

            final ArrayList<Long> requests = new ArrayList<>();
            requests.add(prepareToSendData(serialize("200"), 0L, endpoint, scope));
            requests.add(prepareToSendRemoteKey(objectAddress, endpoint));
            sendData(requests, worker);

            final String statusCode = deserialize(receiveData(10, 0L, worker, scope));
            if (log.isInfoEnabled()) {
                log.info("Received \"{}\"", statusCode);
                log.info("Get operation completed \n");
            }
        } else {
            if (log.isWarnEnabled()) {
                log.warn("Object with key \"{}\" was not found in plasma store", data.get("key"));
            }
            sendSingleMessage(serialize("404"), 0L, endpoint, scope, worker);
            if (log.isInfoEnabled()) {
                log.info("Get operation completed \n");
            }
        }
    }

    private void putOperation(Worker worker, Endpoint endpoint) throws ControlException {
        final MemoryDescriptor descriptor = receiveMemoryDescriptor(0L, worker);
        final byte[] remoteObject = receiveRemoteObject(descriptor, endpoint, worker, scope, resources);
        if (log.isInfoEnabled()) {
            log.info("Read \"{}\" from remote buffer", deserialize(remoteObject).toString());
        }

        final byte[] metadataSizeBytes = receiveData(Integer.BYTES, 0, worker, scope);
        ByteBuffer byteBuffer = ByteBuffer.wrap(metadataSizeBytes);
        int metadataSize = byteBuffer.getInt();
        if (log.isInfoEnabled()) {
            log.info("Received \"{}\"", metadataSize);
        }
        final byte[] metadataBytes = receiveData(metadataSize, 0L, worker, scope);
        HashMap<String, String> metadata = deserialize(metadataBytes);
        if (log.isInfoEnabled()) {
            log.info("Received \"{}\"", metadata);
        }

        byte[] id = new byte[0];
        try {
            id = getMD5Hash(metadata.get("key"));
        } catch (NoSuchAlgorithmException e) {
            if (log.isErrorEnabled()) {
                log.error("The MD5 hash algorithm was not found.", e);
            }
        }
        final byte[] fullID = ArrayUtils.addAll(id, new byte[4]);

        PlasmaEntry newPlasmaEntry = new PlasmaEntry(metadata.get("key"), remoteObject, new byte[0]);
        byte[] newPlasmaEntryBytes = serialize(newPlasmaEntry);

        try {
            saveObjectToPlasma(fullID, newPlasmaEntryBytes, metadataBytes);
        } catch (DuplicateObjectException e) {
            if (log.isWarnEnabled()) {
                log.warn(e.getMessage());
            }
            // TODO: check if hash collision occurred
            PlasmaEntry plasmaEntry = deserialize(plasmaClient.get(fullID, 1, false));
            if (plasmaEntry.getKey().equals(newPlasmaEntry.getKey())) {
                sendSingleMessage(serialize("409"), 0L, endpoint, scope, worker);
                if (log.isInfoEnabled()) {
                    log.info("Put operation completed \n");
                }
                return;
            } else {
                byte[] objectIdWithFreeNextID = traverseEntriesUntilNextIsEmpty(plasmaEntry);
                PlasmaEntry plasmaEntry1 = deserialize(plasmaClient.get(objectIdWithFreeNextID, 1, false));
                String idAsHexString = bytesToHex(objectIdWithFreeNextID);
                log.info(idAsHexString);
                String tailEnd = idAsHexString.substring(idAsHexString.length() - 4);
                log.info(tailEnd);
                Integer tailEndInt = Integer.valueOf(tailEnd);
                tailEndInt += 1;
                String newID = idAsHexString.substring(0, idAsHexString.length() - 4) + tailEndInt;
                log.info(newID);
                byte[] newIdBytes = newID.getBytes(StandardCharsets.UTF_8);
                log.info(bytesToHex(newIdBytes));
                plasmaClient.delete(objectIdWithFreeNextID);
                PlasmaEntry plasmaEntry2 = new PlasmaEntry(plasmaEntry1.key(), plasmaEntry1.value(), newIdBytes);
                saveObjectToPlasma(objectIdWithFreeNextID, serialize(plasmaEntry2), new byte[0]);
                saveObjectToPlasma(newIdBytes, newPlasmaEntryBytes, new byte[0]);
            }

            return;
        }
        sendSingleMessage(serialize("200"), 0L, endpoint, scope, worker);
        if (log.isInfoEnabled()) {
            log.info("Put operation completed \n");
        }
    }

    private byte[] traverseEntriesUntilNextIsEmpty(final PlasmaEntry plasmaEntry) {
        byte[] nextID = plasmaEntry.nextPlasmaID();
        byte[] lastID = nextID;
        while (nextID != null && nextID.length > 0) {
            lastID = nextID;
            final PlasmaEntry nextPlasmaEntry = deserialize(plasmaClient.get(nextID, 1, false));
            nextID = nextPlasmaEntry.nextPlasmaID();
        }
        return lastID;
    }

    private void saveObjectToPlasma(byte[] id, byte[] object, byte[] metadata) throws DuplicateObjectException, PlasmaOutOfMemoryException {
        ByteBuffer byteBuffer = plasmaClient.create(id, object.length, metadata);
        if (log.isInfoEnabled()) log.info("Created new ByteBuffer in plasma store");
        for (byte b : object) {
            byteBuffer.put(b);
        }
        plasmaClient.seal(id);
        log.info("Sealed new object in plasma store");
    }
}
