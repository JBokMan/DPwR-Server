package infinileap.server;


import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import event.listener.OnMessageEventListener;
import jdk.incubator.foreign.MemoryAddress;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class InfinileapServer {

    private final ResourcePool resources = new ResourcePool();
    protected final ResourceScope scope = ResourceScope.newSharedScope();
    private static final long DEFAULT_REQUEST_SIZE = 1024;
    private static final ContextParameters.Feature[] FEATURE_SET = {
            ContextParameters.Feature.TAG, ContextParameters.Feature.RMA, ContextParameters.Feature.WAKEUP, ContextParameters.Feature.AM,
            ContextParameters.Feature.ATOMIC_32, ContextParameters.Feature.ATOMIC_64, ContextParameters.Feature.STREAM
    };
    private static final Identifier IDENTIFIER = new Identifier(0x01);
    private Context context;
    private Worker worker;
    private Endpoint endpoint;
    private final InetSocketAddress listenAddress;
    private Listener listener;
    private final AtomicBoolean messageReceived = new AtomicBoolean(false);

    private OnMessageEventListener onMessageEventListener;

    public InfinileapServer(String listenAddress, Integer listenPort) {
        this.listenAddress = new InetSocketAddress(listenAddress, listenPort);
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
                this.context.createWorker(workerParameters)
        );
    }

    protected <T extends AutoCloseable> T pushResource(T resource) {
        resources.push(resource);
        return resource;
    }

    public void registerOnMessageEventListener(OnMessageEventListener onMessageEventListener) {
        this.onMessageEventListener = onMessageEventListener;
    }

    private void listenLoop() throws ControlException, InterruptedException {
        var connectionRequest = new AtomicReference<ConnectionRequest>();
        var listenerParams = new ListenerParameters()
                .setListenAddress(listenAddress)
                .setConnectionHandler(connectionRequest::set);

        log.info("Listening for new connection requests on {}", listenAddress);
        this.listener = pushResource(this.worker.createListener(listenerParams));

        while (true) {
            Requests.await(this.worker, connectionRequest);

            var endpointParameters = new EndpointParameters()
                    .setConnectionRequest(connectionRequest.get());

            this.endpoint = this.worker.createEndpoint(endpointParameters);

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

        // ToDo get object per RDMA from Client (look at Memory example)
        // Save object to PlasmaStore using MemorySegments

        messageReceived.set(true);
        return Status.OK;
    }
}
