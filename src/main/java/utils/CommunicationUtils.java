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
import java.util.HashMap;
import java.util.List;

import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;

@Slf4j
public class CommunicationUtils {

    public static MemoryDescriptor getMemoryDescriptorOfBytes(final byte[] object, final Context context) throws ControlException {
        final MemorySegment source = MemorySegment.ofArray(object);
        final MemoryRegion memoryRegion = context.allocateMemory(object.length);
        memoryRegion.segment().copyFrom(source);
        return memoryRegion.descriptor();
    }

    public static Long prepareToSendData(final byte[] data, final long tagID, final Endpoint endpoint, final ResourceScope scope) {
        log.info("Prepare to send data");
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

    public static void sendData(final List<Long> requests, final Worker worker) {
        log.info("Sending data");
        for (final Long request : requests) {
            Requests.poll(worker, request);
        }
    }

    public static void sendSingleMessage(final byte[] data, final long tagID, final Endpoint endpoint, final Worker worker) {
        final ResourceScope scope = ResourceScope.newConfinedScope(Cleaner.create());
        final Long request = prepareToSendData(data, tagID, endpoint, scope);
        sendData(List.of(request), worker);
        scope.close();
    }

    public static byte[] receiveData(final int size, final long tagID, final Worker worker) {
        final ResourceScope scope = ResourceScope.newConfinedScope(Cleaner.create());
        final CommunicationBarrier barrier = new CommunicationBarrier();
        final MemorySegment buffer = MemorySegment.allocateNative(size, scope);

        log.info("Receiving message");

        RequestParameters requestParameters = new RequestParameters(scope).setReceiveCallback(barrier::release);
        final long request = worker.receiveTagged(buffer, Tag.of(tagID), requestParameters);

        awaitRequestIfNecessary(request, worker);

        byte[] result = buffer.toArray(ValueLayout.JAVA_BYTE);
        scope.close();
        return result;
    }

    @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
    public static byte[] receiveRemoteObject(final Endpoint endpoint, final Worker worker) throws ControlException {
        final ResourceScope scope = ResourceScope.newConfinedScope(Cleaner.create());

        final MemoryDescriptor descriptor = new MemoryDescriptor(scope);
        log.info("Receiving Remote Key");
        final long request = worker.receiveTagged(descriptor, Tag.of(0L), new RequestParameters(scope));
        awaitRequestIfNecessary(request, worker);

        final MemorySegment targetBuffer = MemorySegment.allocateNative(descriptor.remoteSize(), scope);
        try (final RemoteKey remoteKey = endpoint.unpack(descriptor)) {
            final long request2 = endpoint.get(targetBuffer, descriptor.remoteAddress(), remoteKey, new RequestParameters(scope));
            awaitRequestIfNecessary(request2, worker);
        }

        byte[] result = targetBuffer.toArray(ValueLayout.JAVA_BYTE);
        scope.close();
        return result;
    }

    private static void awaitRequestIfNecessary(final long request, final Worker worker) {
        if (Status.isError(request)) {
            log.warn("A request has an error status");
        }
        Requests.poll(worker, request);
    }

    public static String receiveKey(Worker worker) {
        // Get key size in bytes
        final byte[] keySizeBytes = receiveData(Integer.BYTES, 0, worker);
        ByteBuffer byteBuffer = ByteBuffer.wrap(keySizeBytes);
        int keySize = byteBuffer.getInt();
        log.info("Received \"{}\"", keySize);

        // Get key as bytes
        final byte[] keyBytes = receiveData(keySize, 0L, worker);
        HashMap<String, String> key = deserialize(keyBytes);
        log.info("Received \"{}\"", key);

        return key.get("key");
    }

    public static void sendObjectAddressAndStatusCode(byte[] objectBytes, Endpoint endpoint, Worker worker, Context context, ResourceScope scope) throws ControlException {
        // Prepare objectBytes for transmission
        final MemoryDescriptor objectAddress;
        try {
            objectAddress = getMemoryDescriptorOfBytes(objectBytes, context);
        } catch (ControlException e) {
            log.error("An exception occurred getting the objects memory address, aborting GET operation");
            sendSingleMessage(serialize("500"), 0L, endpoint, worker);
            throw e;
        }

        // Send status and object address
        final ArrayList<Long> requests = new ArrayList<>();
        requests.add(prepareToSendData(serialize("200"), 0L, endpoint, scope));
        requests.add(prepareToSendRemoteKey(objectAddress, endpoint));
        sendData(requests, worker);
    }
}
