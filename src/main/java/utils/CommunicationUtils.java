package utils;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import de.hhu.bsinfo.infinileap.util.CloseException;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import jdk.incubator.foreign.ValueLayout;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.SerializationUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static de.hhu.bsinfo.infinileap.example.util.Requests.state;
import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;
import static utils.PlasmaUtils.deleteById;
import static utils.PlasmaUtils.updateNextIdOfEntry;

@Slf4j
public class CommunicationUtils {

    final private static TimeUnit timeUnit = TimeUnit.MILLISECONDS;

    public static MemoryDescriptor getMemoryDescriptorOfByteBuffer(final ByteBuffer object, final Context context) throws ControlException, CloseException {
        final MemorySegment source = MemorySegment.ofByteBuffer(object);
        try (final MemoryRegion memoryRegion = context.mapMemory(source)) {
            return memoryRegion.descriptor();
        }
    }

    public static Long prepareToSendData(final int tagID, final byte[] data, final Endpoint endpoint, final ResourceScope scope) {
        log.info("Prepare to send data");
        final int dataSize = data.length;

        final MemorySegment source = MemorySegment.ofArray(data);
        final MemorySegment buffer = MemorySegment.allocateNative(dataSize, scope);
        buffer.copyFrom(source);

        return endpoint.sendTagged(buffer, Tag.of(tagID), new RequestParameters());
    }

    public static Long prepareToSendRemoteKey(final int tagID, final MemoryDescriptor descriptor, final Endpoint endpoint) {
        log.info("Prepare to send remote key");
        return endpoint.sendTagged(descriptor, Tag.of(tagID));
    }

    public static void sendRemoteKey(final int tagID, final MemoryDescriptor descriptor, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws TimeoutException {
        log.info("Send remote key");
        final long request = endpoint.sendTagged(descriptor, Tag.of(tagID));
        awaitRequestIfNecessary(request, worker, timeoutMs);
    }

    public static void sendNewEntryAddress(final int tagID, final PlasmaClient plasmaClient, final byte[] id, final int entrySize, final Endpoint endpoint, final Worker worker, final Context context, final int timeoutMs) throws TimeoutException, ControlException, CloseException {
        try {
            // create new plasma entry with correct id and size and send its memory address to client
            final ByteBuffer byteBuffer = plasmaClient.create(id, entrySize, new byte[0]);
            final MemoryDescriptor objectAddress = getMemoryDescriptorOfByteBuffer(byteBuffer, context);
            // signal id was not already in plasma
            sendSingleMessage(tagID, serialize("200"), endpoint, worker, timeoutMs);
            sendRemoteKey(tagID, objectAddress, endpoint, worker, timeoutMs);
        } catch (final TimeoutException e) {
            plasmaClient.seal(id);
            deleteById(plasmaClient, id);
            throw e;
        }
    }

    public static void awaitPutCompletionSignal(final int tagID, final PlasmaClient plasmaClient, final byte[] id, final Worker worker, final byte[] idToUpdate, final int timeoutMs, final int plasmaTimeoutMs) throws TimeoutException {
        final String statusCode;
        try {
            statusCode = SerializationUtils.deserialize(receiveData(tagID, 10, worker, timeoutMs));
        } catch (final TimeoutException e) {
            plasmaClient.seal(id);
            deleteById(plasmaClient, id);
            throw e;
        }
        log.info("Received status code: \"{}\"", statusCode);
        if ("200".equals(statusCode)) {
            plasmaClient.seal(id);
            if (ObjectUtils.isNotEmpty(idToUpdate)) {
                updateNextIdOfEntry(plasmaClient, idToUpdate, id, plasmaTimeoutMs);
            }
        }
    }

    public static int receiveEntrySize(final int tagID, final Worker worker, final int timeoutMs) throws TimeoutException {
        final byte[] entrySizeBytes = receiveData(tagID, Integer.BYTES, worker, timeoutMs);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(entrySizeBytes);
        final int entrySize = byteBuffer.getInt();
        log.info("Received \"{}\"", entrySize);
        return entrySize;
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
                        synchronized (timeUnit) {
                            timeUnit.wait(1);
                        }
                    } catch (final InterruptedException e) {
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
            throw new TimeoutException("A timeout occurred while sending data");
        }
    }

    public static void sendSingleMessage(final int tagID, final byte[] data, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws TimeoutException {
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final Long request = prepareToSendData(tagID, data, endpoint, scope);
            sendData(List.of(request), worker, timeoutMs);
        }
    }

    public static byte[] receiveData(final int tagID, final int size, final Worker worker, final int timeoutMs) throws TimeoutException {
        log.info("Receiving message");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final MemorySegment buffer = MemorySegment.allocateNative(size, scope);
            final long request = worker.receiveTagged(buffer, Tag.of(tagID), new RequestParameters(scope));
            awaitRequestIfNecessary(request, worker, timeoutMs);
            return buffer.toArray(ValueLayout.JAVA_BYTE);
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
                synchronized (timeUnit) {
                    timeUnit.wait(1);
                }
            } catch (final InterruptedException e) {
                log.error(e.getMessage());
                worker.cancelRequest(request);
                return;
            }
            counter++;
        }
        if (state(request) != Requests.State.COMPLETE) {
            worker.cancelRequest(request);
            throw new TimeoutException("A timeout occurred while receiving data");
        } else {
            Requests.release(request);
        }
    }

    public static int receiveTagID(final Worker worker, final int timeoutMs) throws TimeoutException {
        final byte[] tagIDBytes = receiveData(0, Integer.BYTES, worker, timeoutMs);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(tagIDBytes);
        final int tagID = byteBuffer.getInt();
        log.info("Received \"{}\"", tagID);
        return tagID;
    }

    public static String receiveKey(final int tagID, final Worker worker, final int timeoutMs) throws TimeoutException {
        // Get key size in bytes
        final byte[] keySizeBytes = receiveData(tagID, Integer.BYTES, worker, timeoutMs);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(keySizeBytes);
        final int keySize = byteBuffer.getInt();
        log.info("Received \"{}\"", keySize);

        // Get key as bytes
        final byte[] keyBytes = receiveData(tagID, keySize, worker, timeoutMs);
        final String key = deserialize(keyBytes);
        log.info("Received \"{}\"", key);

        return key;
    }

    public static void sendObjectAddressAndStatusCode(final int tagID, final ByteBuffer objectBuffer, final Endpoint endpoint, final Worker worker, final Context context, final int timeoutMs) throws ControlException, TimeoutException, CloseException {
        // Prepare objectBytes for transmission
        final MemoryDescriptor objectAddress;
        try {
            objectAddress = getMemoryDescriptorOfByteBuffer(objectBuffer, context);
        } catch (final ControlException | CloseException e) {
            log.error("An exception occurred getting the objects memory address, aborting GET operation");
            sendSingleMessage(tagID, serialize("500"), endpoint, worker, timeoutMs);
            throw e;
        }

        // Send status and object address
        final ArrayList<Long> requests = new ArrayList<>();
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requests.add(prepareToSendData(tagID, serialize("200"), endpoint, scope));
            requests.add(prepareToSendRemoteKey(tagID, objectAddress, endpoint));
            sendData(requests, worker, timeoutMs);
        }
    }
}
