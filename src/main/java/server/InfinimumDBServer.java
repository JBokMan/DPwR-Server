package server;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import jdk.incubator.foreign.MemoryAddress;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class InfinimumDBServer {

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
    private static final Identifier IDENTIFIER = new Identifier(0x01);
    private Worker worker;
    private final InetSocketAddress listenAddress;
    private final AtomicBoolean messageReceived = new AtomicBoolean(false);

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
        Context context = pushResource(
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

        while (true) {
            Requests.await(this.worker, connectionRequest);

            var endpointParameters = new EndpointParameters()
                    .setConnectionRequest(connectionRequest.get());

            this.worker.createEndpoint(endpointParameters);

            // Register local handler
            var params = new HandlerParameters()
                    .setId(IDENTIFIER)
                    .setCallback(this::onActiveMessage)
                    .setFlags(HandlerParameters.Flag.WHOLE_MESSAGE);

            this.worker.setHandler(params);

            while (!messageReceived.get()) {
                this.worker.progress();
            }
            messageReceived.set(false);
            connectionRequest.set(null);
        }
    }

    private Status onActiveMessage(MemoryAddress argument, MemorySegment header, MemorySegment data, MemoryAddress params) {
        log.info("Received operation name {} in header", header.getUtf8String(0L));
        String[] temp = data.getUtf8String(0L).split(",");
        log.info("Received size {} and memory address {} in body", temp[0], temp[1]);
        int size = Integer.parseInt(temp[0]);

        // ToDo get object per RDMA from Client (look at Memory example)
        // Save object to PlasmaStore using MemorySegments

        String object = "Hello World Man!";
        byte[] objectID = generateUUID(object.getBytes());
        ByteBuffer byteBuffer = plasmaClient.create(objectID, size, new byte[0]);
        for (byte b : object.getBytes()) {
            byteBuffer.put(b);
        }
        plasmaClient.seal(objectID);


        messageReceived.set(true);
        return Status.OK;
    }
}
