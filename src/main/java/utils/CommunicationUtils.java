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
import org.apache.commons.lang3.SerializationException;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    private static Long prepareToSendData(final int tagID, final byte[] data, final Endpoint endpoint, final ResourceScope scope) {
        log.info("Prepare to send data");
        final int dataSize = data.length;

        final MemorySegment source = MemorySegment.ofArray(data);
        final MemorySegment buffer = MemorySegment.allocateNative(dataSize, scope);
        buffer.copyFrom(source);

        return endpoint.sendTagged(buffer, Tag.of(tagID), new RequestParameters(scope));
    }

    private static Long prepareToSendStatusCode(final int tagID, final String string, final Endpoint endpoint, final ResourceScope scope) {
        return prepareToSendData(tagID, serialize(string), endpoint, scope);
    }

    private static Long prepareToSendInteger(final int tagID, final int integer, final Endpoint endpoint, final ResourceScope scope) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(integer);
        return prepareToSendData(tagID, byteBuffer.array(), endpoint, scope);
    }

    private static ArrayList<Long> prepareToSendAddress(final int tagID, final InetSocketAddress address, final Endpoint endpoint, final ResourceScope scope) {
        final ArrayList<Long> requests = new ArrayList<>();
        final byte[] addressBytes = serialize(address);
        requests.add(prepareToSendInteger(tagID, addressBytes.length, endpoint, scope));
        requests.add(prepareToSendData(tagID, addressBytes, endpoint, scope));
        return requests;
    }

    private static Long prepareToSendRemoteKey(final int tagID, final MemoryDescriptor descriptor, final Endpoint endpoint) {
        log.info("Prepare to send remote key");
        return endpoint.sendTagged(descriptor, Tag.of(tagID));
    }

    private static void awaitRequest(final long request, final Worker worker, final int timeoutMs) throws TimeoutException, InterruptedException {
        int counter = 0;
        while (state(request) != Requests.State.COMPLETE && counter < timeoutMs) {
            worker.progress();
            synchronized (timeUnit) {
                timeUnit.wait(1);
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

    private static void awaitRequests(final List<Long> requests, final Worker worker, final int timeoutMs) throws TimeoutException {
        log.info("Sending data");
        boolean timeoutHappened = false;
        for (final Long request : requests) {
            if (timeoutHappened) {
                worker.cancelRequest(request);
                continue;
            }

            try {
                awaitRequest(request, worker, timeoutMs);
            } catch (final TimeoutException | InterruptedException e) {
                timeoutHappened = true;
            }
        }

        if (timeoutHappened) {
            throw new TimeoutException("A timeout occurred while sending data");
        }
    }

    private static void sendSingleByteArray(final int tagID, final byte[] data, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws TimeoutException {
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final Long request = prepareToSendData(tagID, data, endpoint, scope);
            awaitRequests(List.of(request), worker, timeoutMs);
        }
    }

    public static void sendSingleInteger(final int tagID, final int integer, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws TimeoutException {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(integer);
        sendSingleByteArray(tagID, byteBuffer.array(), endpoint, worker, timeoutMs);
    }

    private static void sendSingleString(final int tagID, final String string, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws TimeoutException {
        sendSingleByteArray(tagID, serialize(string), endpoint, worker, timeoutMs);
    }

    public static void sendStatusCode(final int tagID, final String statusCode, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws TimeoutException {
        sendSingleString(tagID, statusCode, endpoint, worker, timeoutMs);
    }

    public static void sendOperationName(final int tagID, final String operationName, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws TimeoutException {
        sendSingleString(tagID, operationName, endpoint, worker, timeoutMs);
    }

    public static void sendObjectAddress(final int tagID, final ByteBuffer objectBuffer, final Endpoint endpoint, final Worker worker, final Context context, final int timeoutMs) throws CloseException, ControlException, TimeoutException {
        final MemoryDescriptor objectAddress = getMemoryDescriptorOfByteBuffer(objectBuffer, context);
        final Long request = prepareToSendRemoteKey(tagID, objectAddress, endpoint);
        awaitRequests(List.of(request), worker, timeoutMs);
    }

    public static void sendAddress(final int tagID, final InetSocketAddress address, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws TimeoutException {
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final ArrayList<Long> requests = new ArrayList<>();
            final byte[] addressBytes = serialize(address);
            requests.add(prepareToSendInteger(tagID, addressBytes.length, endpoint, scope));
            requests.add(prepareToSendData(tagID, addressBytes, endpoint, scope));
            awaitRequests(requests, worker, timeoutMs);
        }
    }

    public static void sendServerMap(final int tagID, final Map<Integer, InetSocketAddress> serverMap, final Worker worker, final Endpoint endpoint, final int currentServerCount, final int timeoutMs) throws TimeoutException {
        final ArrayList<Long> requests = new ArrayList<>();
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requests.add(prepareToSendStatusCode(tagID, "200", endpoint, scope));
            requests.add(prepareToSendInteger(tagID, currentServerCount, endpoint, scope));
            for (int i = 0; i < currentServerCount; i++) {
                requests.addAll(prepareToSendAddress(tagID, serverMap.get(i), endpoint, scope));
            }
            awaitRequests(requests, worker, timeoutMs);
        }
    }

    private static MemoryDescriptor getMemoryDescriptorOfByteBuffer(final ByteBuffer object, final Context context) throws ControlException, CloseException {
        final MemorySegment source = MemorySegment.ofByteBuffer(object);
        try (final MemoryRegion memoryRegion = context.mapMemory(source)) {
            return memoryRegion.descriptor();
        }
    }

    //todo rename
    public static void sendNewEntryAddress(final int tagID, final PlasmaClient plasmaClient, final byte[] id, final int entrySize, final Endpoint endpoint, final Worker worker, final Context context, final int timeoutMs) throws TimeoutException, ControlException, CloseException {
        try {
            // create new plasma entry with correct id and size and send its memory address to client
            final ByteBuffer byteBuffer = plasmaClient.create(id, entrySize, new byte[0]);
            sendStatusCode(tagID, "200", endpoint, worker, timeoutMs);
            sendObjectAddress(tagID, byteBuffer, endpoint, worker, context, timeoutMs);
        } catch (final TimeoutException e) {
            plasmaClient.seal(id);
            deleteById(plasmaClient, id);
            throw e;
        }
    }

    private static byte[] receiveData(final int tagID, final int size, final Worker worker, final int timeoutMs) throws TimeoutException {
        log.info("Receiving message");
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final MemorySegment buffer = MemorySegment.allocateNative(size, scope);
            final long request = worker.receiveTagged(buffer, Tag.of(tagID), new RequestParameters(scope));
            awaitRequests(List.of(request), worker, timeoutMs);
            return buffer.toArray(ValueLayout.JAVA_BYTE);
        }
    }

    public static int receiveInteger(final int tagID, final Worker worker, final int timeoutMs) throws TimeoutException {
        final byte[] integerBytes = receiveData(tagID, Integer.BYTES, worker, timeoutMs);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(integerBytes);
        final int number = byteBuffer.getInt();
        log.info("Received \"{}\"", number);
        return number;
    }

    public static int receiveTagID(final Worker worker, final int timeoutMs) throws TimeoutException {
        return receiveInteger(0, worker, timeoutMs);
    }

    public static InetSocketAddress receiveAddress(final int tagID, final Worker worker, final int timeoutMs) throws TimeoutException {
        final int addressSize = receiveInteger(tagID, worker, timeoutMs);
        final byte[] serverAddressBytes = receiveData(tagID, addressSize, worker, timeoutMs);
        return deserialize(serverAddressBytes);
    }

    public static String receiveKey(final int tagID, final Worker worker, final int timeoutMs) throws TimeoutException {
        // Get key size in bytes
        final int keySize = receiveInteger(tagID, worker, timeoutMs);

        // Get key as bytes
        final byte[] keyBytes = receiveData(tagID, keySize, worker, timeoutMs);
        final String key = deserialize(keyBytes);
        log.info("Received \"{}\"", key);

        return key;
    }

    public static String receiveStatusCode(final int tagID, final Worker worker, final int timeoutMs) throws TimeoutException, SerializationException {
        final byte[] statusCodeBytes = receiveData(tagID, 10, worker, timeoutMs);
        final String statusCode = deserialize(statusCodeBytes);
        log.info("Received status code: \"{}\"", statusCode);
        return statusCode;
    }

    public static String receiveOperationName(final int tagID, final Worker worker, final int timeoutMs) throws TimeoutException, SerializationException {
        final byte[] operationNameBytes = receiveData(tagID, 10, worker, timeoutMs);
        final String operationName = deserialize(operationNameBytes);
        log.info("Received operation name: \"{}\"", operationName);
        return operationName;
    }

    public static void awaitPutCompletionSignal(final int tagID, final PlasmaClient plasmaClient, final byte[] id, final Worker worker, final byte[] idToUpdate, final int timeoutMs, final int plasmaTimeoutMs) throws TimeoutException, SerializationException {
        final String statusCode;
        try {
            statusCode = receiveStatusCode(tagID, worker, timeoutMs);
        } catch (final TimeoutException | SerializationException e) {
            plasmaClient.seal(id);
            deleteById(plasmaClient, id);
            throw e;
        }
        if ("200".equals(statusCode)) {
            plasmaClient.seal(id);
            if (ObjectUtils.isNotEmpty(idToUpdate)) {
                updateNextIdOfEntry(plasmaClient, idToUpdate, id, plasmaTimeoutMs);
            }
        }
    }
}
