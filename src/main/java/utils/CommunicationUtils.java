package utils;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.CommunicationBarrier;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import jdk.incubator.foreign.ValueLayout;
import lombok.extern.slf4j.Slf4j;

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static de.hhu.bsinfo.infinileap.example.util.Requests.state;
import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;

@Slf4j
public class CommunicationUtils {

    public static MemoryDescriptor getMemoryDescriptorOfByteBuffer(final ByteBuffer object, final Context context) throws ControlException {
        final MemorySegment source = MemorySegment.ofByteBuffer(object);
        final MemoryRegion memoryRegion = context.mapMemory(source);
        return memoryRegion.descriptor();
    }

    public static Long prepareToSendData(final byte[] data, final long tagID, final Endpoint endpoint) {
        log.info("Prepare to send data");
        final ResourceScope scope = ResourceScope.newConfinedScope(Cleaner.create());
        final int dataSize = data.length;

        final MemorySegment source = MemorySegment.ofArray(data);
        final MemorySegment buffer = MemorySegment.allocateNative(dataSize, scope);
        buffer.copyFrom(source);

        return endpoint.sendTagged(buffer, Tag.of(tagID), new RequestParameters());
    }

    public static Long prepareToSendRemoteKey(final MemoryDescriptor descriptor, final Endpoint endpoint) {
        log.info("Prepare to send remote key");
        return endpoint.sendTagged(descriptor, Tag.of(0L));
    }

    public static void sendData(final List<Long> requests, final Worker worker, final int timeoutMs) throws TimeoutException {
        log.info("Sending data");
        boolean timeoutHappened = false;
        for (final Long request : requests) {
            if (timeoutHappened) {
                worker.cancelRequest(request);
            } else {
                int counter = 0;
                while (state(request) != Requests.State.COMPLETE && counter < timeoutMs) {
                    worker.progress();
                    try {
                        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
                        synchronized (timeUnit) {
                            timeUnit.wait(1);
                        }
                    } catch (InterruptedException e) {
                        log.error(e.getMessage());
                        worker.cancelRequest(request);
                        timeoutHappened = true;
                        continue;
                    }
                    counter++;
                }
                if (state(request) != Requests.State.COMPLETE) {
                    worker.cancelRequest(request);
                    timeoutHappened = true;
                } else {
                    Requests.release(request);
                }
            }
        }
        if (timeoutHappened) {
            throw new TimeoutException();
        }
    }

    public static void sendSingleMessage(final byte[] data, final long tagID, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws TimeoutException {
        final ResourceScope scope = ResourceScope.newConfinedScope(Cleaner.create());
        final Long request = prepareToSendData(data, tagID, endpoint);
        sendData(List.of(request), worker, timeoutMs);
        scope.close();
    }

    public static byte[] receiveData(final int size, final long tagID, final Worker worker, final int timeoutMs) throws TimeoutException {
        try (final ResourceScope scope = ResourceScope.newConfinedScope(Cleaner.create())) {
            final CommunicationBarrier barrier = new CommunicationBarrier();
            final MemorySegment buffer = MemorySegment.allocateNative(size, scope);

            log.info("Receiving message");

            RequestParameters requestParameters = new RequestParameters(scope).setReceiveCallback(barrier::release);
            final long request = worker.receiveTagged(buffer, Tag.of(tagID), requestParameters);

            awaitRequestIfNecessary(request, worker, timeoutMs);

            return buffer.toArray(ValueLayout.JAVA_BYTE);
        }
    }

    public static byte[] receiveRemoteObject(final Endpoint endpoint, final Worker worker, final int timeoutMs) throws ControlException, TimeoutException {
        try (final ResourceScope scope = ResourceScope.newConfinedScope(Cleaner.create())) {

            final MemoryDescriptor descriptor = new MemoryDescriptor(scope);
            log.info("Receiving Remote Key");
            final long request = worker.receiveTagged(descriptor, Tag.of(0L), new RequestParameters(scope));

            awaitRequestIfNecessary(request, worker, timeoutMs);

            final MemorySegment targetBuffer = MemorySegment.allocateNative(descriptor.remoteSize(), scope);
            try (final RemoteKey remoteKey = endpoint.unpack(descriptor)) {
                final long request2 = endpoint.get(targetBuffer, descriptor.remoteAddress(), remoteKey, new RequestParameters(scope));
                awaitRequestIfNecessary(request2, worker, timeoutMs);
            }

            return targetBuffer.toArray(ValueLayout.JAVA_BYTE);
        }
    }

    private static void awaitRequestIfNecessary(final long request, final Worker worker, final int timeoutMs) throws TimeoutException {
        if (Status.isError(request)) {
            log.warn("A request has an error status");
        }
        int counter = 0;
        while (state(request) != Requests.State.COMPLETE && counter < timeoutMs) {
            worker.progress();
            try {
                TimeUnit timeUnit = TimeUnit.MILLISECONDS;
                synchronized (timeUnit) {
                    timeUnit.wait(1);
                }
            } catch (InterruptedException e) {
                log.error(e.getMessage());
                worker.cancelRequest(request);
                return;
            }
            counter++;
        }
        if (state(request) != Requests.State.COMPLETE) {
            worker.cancelRequest(request);
            throw new TimeoutException();
        } else {
            Requests.release(request);
        }
    }

    public static String receiveKey(Worker worker, final int timeoutMs) throws TimeoutException {
        // Get key size in bytes
        final byte[] keySizeBytes = receiveData(Integer.BYTES, 0, worker, timeoutMs);
        ByteBuffer byteBuffer = ByteBuffer.wrap(keySizeBytes);
        int keySize = byteBuffer.getInt();
        log.info("Received \"{}\"", keySize);

        // Get key as bytes
        final byte[] keyBytes = receiveData(keySize, 0L, worker, timeoutMs);
        String key = deserialize(keyBytes);
        log.info("Received \"{}\"", key);

        return key;
    }

    public static void sendObjectAddressAndStatusCode(ByteBuffer objectBuffer, Endpoint endpoint, Worker worker, Context context, final int timeoutMs) throws ControlException, TimeoutException {
        // Prepare objectBytes for transmission
        final MemoryDescriptor objectAddress;
        try {
            objectAddress = getMemoryDescriptorOfByteBuffer(objectBuffer, context);
        } catch (ControlException e) {
            log.error("An exception occurred getting the objects memory address, aborting GET operation");
            sendSingleMessage(serialize("500"), 0L, endpoint, worker, timeoutMs);
            throw e;
        }

        // Send status and object address
        final ArrayList<Long> requests = new ArrayList<>();
        requests.add(prepareToSendData(serialize("200"), 0L, endpoint));
        requests.add(prepareToSendRemoteKey(objectAddress, endpoint));
        sendData(requests, worker, timeoutMs);
    }
}
