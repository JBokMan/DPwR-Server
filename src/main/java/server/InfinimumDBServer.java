package server;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.CommunicationBarrier;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import jdk.incubator.foreign.ValueLayout;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;
import org.apache.commons.lang3.SerializationUtils;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class InfinimumDBServer {

    private static final int OPERATION_MESSAGE_SIZE = 10;
    final int serverID = 0;
    int serverCount = 1;

    private PlasmaClient plasmaClient;
    private final String plasmaFilePath;

    private final ResourcePool resources = new ResourcePool();
    protected final ResourceScope scope = ResourceScope.newSharedScope();
    private static final long DEFAULT_REQUEST_SIZE = 1024;
    private static final ContextParameters.Feature[] FEATURE_SET = {
            ContextParameters.Feature.TAG, ContextParameters.Feature.RMA, ContextParameters.Feature.WAKEUP, ContextParameters.Feature.AM,
            ContextParameters.Feature.ATOMIC_32, ContextParameters.Feature.ATOMIC_64, ContextParameters.Feature.STREAM
    };
    private Worker worker;
    private final InetSocketAddress listenAddress;
    private Endpoint endpoint;
    private Context context;
    private final CommunicationBarrier barrier = new CommunicationBarrier();

    public InfinimumDBServer(String plasmaFilePath, String listenAddress, Integer listenPort) {
        this.listenAddress = new InetSocketAddress(listenAddress, listenPort);
        this.plasmaFilePath = plasmaFilePath;
        connectPlasma();
        listen();
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
            System.err.println("PlasmaDB could not be reached");
        }
    }

    public void put(byte[] id, byte[] object) {
        try {
            this.plasmaClient.put(id, object, new byte[0]);
        } catch (DuplicateObjectException e) {
            this.plasmaClient.delete(id);
            this.put(id, object);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public byte[] get(byte[] uuid) {
        try {
            return this.plasmaClient.get(uuid, 100, false);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        return new byte[0];
    }

    public byte[] generateUUID(byte[] object) {
        UUID uuidOfObject = UUID.nameUUIDFromBytes(object);
        ByteBuffer bb = ByteBuffer.wrap(new byte[20]);
        bb.putLong(uuidOfObject.getMostSignificantBits());
        bb.putLong(uuidOfObject.getLeastSignificantBits());
        return bb.array();
    }

    public boolean isThisServerResponsible(byte[] object) {
        int responsibleServerID = Math.abs(Arrays.hashCode(object) % serverCount);
        return this.serverID == responsibleServerID;
    }

    public void listen() {
        NativeLogger.enable();
        log.info("Using UCX version {}", Context.getVersion());
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
        var contextParameters = new ContextParameters()
                .setFeatures(FEATURE_SET)
                .setRequestSize(DEFAULT_REQUEST_SIZE);

        // Read configuration (Environment Variables)
        var configuration = pushResource(
                Configuration.read()
        );

        log.info("Initializing context");

        // Initialize UCP context
        this.context = pushResource(
                Context.initialize(contextParameters, configuration)
        );

        var workerParameters = new WorkerParameters()
                .setThreadMode(ThreadMode.SINGLE);

        log.info("Creating worker");

        // Create a worker
        this.worker = pushResource(
                context.createWorker(workerParameters)
        );
    }

    protected <T extends AutoCloseable> T pushResource(T resource) {
        resources.push(resource);
        return resource;
    }

    private void listenLoop() throws ControlException, InterruptedException {
        var connectionRequest = new AtomicReference<ConnectionRequest>();
        var listenerParams = new ListenerParameters()
                .setListenAddress(listenAddress)
                .setConnectionHandler(connectionRequest::set);

        log.info("Listening for new connection requests on {}", listenAddress);
        pushResource(this.worker.createListener(listenerParams));

        long tagID = 0;
        while (true) {
            Requests.await(this.worker, connectionRequest);

            var endpointParameters = new EndpointParameters()
                    .setConnectionRequest(connectionRequest.get());
            Endpoint endpoint = this.worker.createEndpoint(endpointParameters);

            handleMessage(context, worker, endpoint, scope, tagID);

            connectionRequest.set(null);
            tagID++;
        }
    }

    private void handleMessage(Context context, Worker worker, Endpoint endpoint, ResourceScope scope, long tagID) {
        final CommunicationBarrier barrier = new CommunicationBarrier();
        // Allocate a buffer for receiving the remote's message
        var buffer = MemorySegment.allocateNative(OPERATION_MESSAGE_SIZE, scope);

        // Receive the message
        log.info("Receiving message");
        System.out.println(tagID);

        var request = worker.receiveTagged(buffer, Tag.of(tagID), new RequestParameters()
                .setReceiveCallback(barrier::release));

        try {
            Requests.await(worker, barrier);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("2");
        Requests.release(request);

        String operationName =
                SerializationUtils.deserialize(buffer.toArray(ValueLayout.JAVA_BYTE));
        log.info("Received \"{}\"", operationName);
        switch (operationName) {
            case "PUT" -> {
                log.info("Start PUT operation");
                putOperation(worker, endpoint);
            }
            default -> {
            }
        }
    }

    private void putOperation(Worker worker, Endpoint endpoint) {
        // Allocate a memory descriptor
        var descriptor = new MemoryDescriptor();

        // Receive the message
        log.info("Receiving Remote Key");

        var request = worker.receiveTagged(descriptor, Tag.of(0L), new RequestParameters()
                .setReceiveCallback(barrier::release));

        try {
            Requests.await(worker, barrier);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Requests.release(request);

        // Read remote memory
        RemoteKey remoteKey = null;
        try {
            remoteKey = endpoint.unpack(descriptor);
        } catch (ControlException e) {
            e.printStackTrace();
        }
        if (remoteKey == null) {
            log.error("Remote key was null");
            return;
        }
        var targetBuffer = MemorySegment.allocateNative(descriptor.remoteSize(), scope);
        pushResource(remoteKey);

        request = endpoint.get(targetBuffer, descriptor.remoteAddress(), remoteKey, new RequestParameters()
                .setReceiveCallback(barrier::release));

        try {
            Requests.await(worker, barrier);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Requests.release(request);

        log.info("Read \"{}\" from remote buffer", SerializationUtils.deserialize(targetBuffer.toArray(ValueLayout.JAVA_BYTE)).toString());

        // Signal completion
        final var completion = MemorySegment.allocateNative(Byte.BYTES, scope);
        request = endpoint.sendTagged(completion, Tag.of(0L));

        try {
            Requests.await(worker, request);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        byte[] object = targetBuffer.toArray(ValueLayout.JAVA_BYTE);
        byte[] objectID = generateUUID(object);
        ByteBuffer byteBuffer = plasmaClient.create(objectID, object.length, new byte[0]);

        log.info("Created new ByteBuffer in plasma store");

        for (byte b : object) {
            byteBuffer.put(b);
        }
        plasmaClient.seal(objectID);

        log.info("Sealed new object in plasma store");

        Requests.release(request);
    }
}
