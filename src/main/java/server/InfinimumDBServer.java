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
import org.apache.commons.lang3.SerializationUtils;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

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
    private static final ContextParameters.Feature[] FEATURE_SET = {
            ContextParameters.Feature.TAG, ContextParameters.Feature.RMA, ContextParameters.Feature.WAKEUP, ContextParameters.Feature.AM,
            ContextParameters.Feature.ATOMIC_32, ContextParameters.Feature.ATOMIC_64, ContextParameters.Feature.STREAM
    };
    private transient Worker worker;
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

    public byte[] get(byte[] uuid) {
        try {
            return this.plasmaClient.get(uuid, 100, false);
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error(e.getMessage());
            }
        }
        return new byte[0];
    }

    public boolean isThisServerResponsible(byte[] object) {
        int responsibleServerID = Math.abs(Arrays.hashCode(object) % serverCount);
        return this.serverID == responsibleServerID;
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

        Thread cleanUpThread = new Thread(() -> {
            if (log.isWarnEnabled()) {
                log.warn("Cleanup");
            }
            try {
                resources.close();
            } catch (CloseException e) {
                if (log.isErrorEnabled()) {
                    log.error("Exception while cleaning up");
                }
            }
        });
        Runtime.getRuntime().addShutdownHook(cleanUpThread);
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
            // ToDo create worker pool and create endpoint from free worker
            Endpoint endpoint = this.worker.createEndpoint(endpointParameters);
            String operationName = SerializationUtils.deserialize(receiveData(OPERATION_MESSAGE_SIZE, 0L, worker, scope));

            log.info("Received \"{}\"", operationName);
            switch (operationName) {
                case "PUT" -> {
                    log.info("Start PUT operation");
                    putOperation(worker, endpoint);
                }
                default -> {
                }
            }
            endpoint.close();
            connectionRequest.set(null);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private void putOperation(Worker worker, Endpoint endpoint) {
        final byte[] id = receiveData(16, 0, worker, scope);
        if (log.isInfoEnabled()) {
            log.info("Received \"{}\"", bytesToHex(id));
        }
        final byte[] fullID = ArrayUtils.addAll(id, new byte[4]);

        final MemoryDescriptor descriptor = receiveMemoryDescriptor(0L, worker);
        final byte[] remoteObject = receiveRemoteObject(descriptor, endpoint, worker, scope, resources);

        if (log.isInfoEnabled()) {
            log.info("Read \"{}\" from remote buffer", SerializationUtils.deserialize(remoteObject).toString());
        }
        try {
            saveObjectToPlasma(fullID, remoteObject);
        } catch (DuplicateObjectException e) {
            if (log.isWarnEnabled()) {
                log.warn(e.getMessage());
            }
        }

        ArrayList<Long> requests = new ArrayList<>();
        requests.add(prepareToSendData(SerializationUtils.serialize("200"), 0L, endpoint, scope));
        requests.add(prepareToSendData(SerializationUtils.serialize(this.serverID), 0L, endpoint, scope));
        sendData(requests, worker);

        log.info("Put operation completed");
    }

    private void saveObjectToPlasma(byte[] fullID, byte[] remoteObject) throws DuplicateObjectException, PlasmaOutOfMemoryException {
        ByteBuffer byteBuffer = plasmaClient.create(fullID, remoteObject.length, new byte[0]);
        if (log.isInfoEnabled()) log.info("Created new ByteBuffer in plasma store");
        for (byte b : remoteObject) {
            byteBuffer.put(b);
        }
        plasmaClient.seal(fullID);
        log.info("Sealed new object in plasma store");
    }
}
