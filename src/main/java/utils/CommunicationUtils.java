package utils;

import de.hhu.bsinfo.infinileap.binding.Context;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.binding.Endpoint;
import de.hhu.bsinfo.infinileap.binding.MemoryDescriptor;
import de.hhu.bsinfo.infinileap.binding.MemoryRegion;
import de.hhu.bsinfo.infinileap.binding.RequestParameters;
import de.hhu.bsinfo.infinileap.binding.Tag;
import de.hhu.bsinfo.infinileap.binding.Worker;
import de.hhu.bsinfo.infinileap.util.CloseException;
import de.hhu.bsinfo.infinileap.util.Requests;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import jdk.incubator.foreign.ValueLayout;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.SerializationException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static de.hhu.bsinfo.infinileap.util.Requests.State.COMPLETE;
import static de.hhu.bsinfo.infinileap.util.Requests.State.ERROR;
import static de.hhu.bsinfo.infinileap.util.Requests.state;
import static org.apache.commons.lang3.SerializationUtils.deserialize;
import static org.apache.commons.lang3.SerializationUtils.serialize;
import static utils.PlasmaUtils.deleteById;
import static utils.PlasmaUtils.updateNextIdOfEntry;

@Slf4j
public class CommunicationUtils {

    private static Long prepareToSendData(final int tagID, final byte[] data, final Endpoint endpoint, final ResourceScope scope) {
        log.info("[{}] Prepare to send data", tagID);
        final int dataSize = data.length;

        final MemorySegment source = MemorySegment.ofArray(data);
        final MemorySegment buffer = MemorySegment.allocateNative(dataSize, scope);
        buffer.copyFrom(source);

        return endpoint.sendTagged(buffer, Tag.of(tagID), new RequestParameters(scope));
    }

    private static Long prepareToSendInteger(final int tagID, final int integer, final Endpoint endpoint, final ResourceScope scope) {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(integer);
        return prepareToSendData(tagID, byteBuffer.array(), endpoint, scope);
    }

    private static long[] prepareToSendAddress(final int tagID, final InetSocketAddress address, final Endpoint endpoint, final ResourceScope scope) {
        final long[] requests = new long[2];
        final byte[] addressBytes = serialize(address);
        requests[0] = prepareToSendInteger(tagID, addressBytes.length, endpoint, scope);
        requests[1] = prepareToSendData(tagID, addressBytes, endpoint, scope);
        return requests;
    }

    private static Long prepareToSendRemoteKey(final int tagID, final MemoryDescriptor descriptor, final Endpoint endpoint) {
        log.info("[{}] Prepare to send remote key", tagID);
        return endpoint.sendTagged(descriptor, Tag.of(tagID));
    }

    private static void awaitRequest(final long request, final Worker worker, final int timeoutMs) throws TimeoutException {
        final long timeout = 1_000L * timeoutMs;
        int counter = 0;
        Requests.State requestState = state(request);
        while ((requestState != COMPLETE) && (requestState != ERROR) && (counter < timeout)) {
            worker.progress();
            counter++;
            requestState = state(request);
        }
        if (requestState != COMPLETE) {
            worker.cancelRequest(request);
            throw new TimeoutException("A timeout occurred while awaiting a request");
        } else {
            Requests.release(request);
        }
    }

    public static void awaitRequests(final long[] requests, final Worker worker, final int timeoutMs) throws TimeoutException {
        boolean timeoutHappened = false;
        for (int i = 0; i < requests.length; i++) {
            if (timeoutHappened) {
                worker.cancelRequest(requests[i]);
                continue;
            }
            try {
                awaitRequest(requests[i], worker, timeoutMs);
            } catch (final TimeoutException e) {
                timeoutHappened = true;
                worker.cancelRequest(requests[i]);
            }
        }
        if (timeoutHappened) {
            log.error("A timeout occurred while sending data");
            throw new TimeoutException("A timeout occurred while sending data");
        }
    }

    private static void sendSingleByteArray(final int tagID, final byte[] data, final Endpoint endpoint, final Worker worker, final int timeoutMs, final ResourceScope scope) throws TimeoutException {
        final long request = prepareToSendData(tagID, data, endpoint, scope);
        awaitRequests(new long[]{request}, worker, timeoutMs);
    }

    public static void sendSingleInteger(final int tagID, final int integer, final Endpoint endpoint, final Worker worker, final int timeoutMs, final ResourceScope scope) throws TimeoutException {
        log.info("[{}] send number {}", tagID, integer);
        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(integer);
        sendSingleByteArray(tagID, byteBuffer.array(), endpoint, worker, timeoutMs, scope);
    }

    private static void sendSingleString(final int tagID, final String string, final Endpoint endpoint, final Worker worker, final int timeoutMs, final ResourceScope scope) throws TimeoutException {
        sendSingleByteArray(tagID, ByteBuffer.wrap(new byte[6]).putChar(string.charAt(0)).putChar(string.charAt(1)).putChar(string.charAt(2)).array(), endpoint, worker, timeoutMs, scope);
    }

    public static void sendStatusCode(final int tagID, final String statusCode, final Endpoint endpoint, final Worker worker, final int timeoutMs, final ResourceScope scope) throws TimeoutException {
        sendSingleString(tagID, statusCode, endpoint, worker, timeoutMs, scope);
    }

    public static void sendOperationName(final int tagID, final String operationName, final Endpoint endpoint, final Worker worker, final int timeoutMs, final ResourceScope scope) throws TimeoutException {
        sendSingleString(tagID, operationName, endpoint, worker, timeoutMs, scope);
    }

    private static MemoryDescriptor getMemoryDescriptorOfByteBuffer(final ByteBuffer object, final Context context) throws ControlException, CloseException {
        final MemorySegment source = MemorySegment.ofByteBuffer(object);
        try (final MemoryRegion memoryRegion = context.mapMemory(source)) {
            return memoryRegion.descriptor();
        }
    }

    private static MemoryDescriptor getMemoryDescriptorOfByteArray(final byte[] object, final Context context) throws ControlException, CloseException {
        final MemorySegment source = MemorySegment.ofArray(object);
        try (final MemoryRegion memoryRegion = context.allocateMemory(object.length)) {
            memoryRegion.segment().copyFrom(source);
            return memoryRegion.descriptor();
        }
    }

    public static void sendObjectAddress(final int tagID, final ByteBuffer objectBuffer, final Endpoint endpoint, final Worker worker, final Context context, final int timeoutMs) throws CloseException, ControlException, TimeoutException {
        log.info("[{}] Send object address", tagID);
        MemoryDescriptor objectAddress = null;
        try {
            objectAddress = getMemoryDescriptorOfByteBuffer(objectBuffer, context);
        } catch (final UnsupportedOperationException e) {
            log.error(e.getMessage());
        }
        final long request = prepareToSendRemoteKey(tagID, objectAddress, endpoint);
        awaitRequests(new long[]{request}, worker, timeoutMs);
    }

    public static void sendObjectAddress(final int tagID, final byte[] object, final Endpoint endpoint, final Worker worker, final Context context, final int timeoutMs) throws CloseException, ControlException, TimeoutException {
        log.info("[{}] Send object address", tagID);
        MemoryDescriptor objectAddress = null;
        try {
            objectAddress = getMemoryDescriptorOfByteArray(object, context);
        } catch (final UnsupportedOperationException e) {
            log.error(e.getMessage());
        }
        final long request = prepareToSendRemoteKey(tagID, objectAddress, endpoint);
        awaitRequests(new long[]{request}, worker, timeoutMs);
    }

    public static void createEntryAndSendNewEntryAddress(final int tagID, final PlasmaClient plasmaClient, final byte[] id, final int entrySize, final Endpoint endpoint, final Worker worker, final Context context, final int timeoutMs, final ResourceScope scope) throws TimeoutException, ControlException, CloseException {
        // create new plasma entry with correct id and size and send its memory address to client
        final ByteBuffer byteBuffer = plasmaClient.create(id, entrySize, new byte[0]);
        try {
            sendStatusCode(tagID, "200", endpoint, worker, timeoutMs, scope);
            sendObjectAddress(tagID, byteBuffer, endpoint, worker, context, timeoutMs);
        } catch (final TimeoutException e) {
            plasmaClient.seal(id);
            deleteById(plasmaClient, id);
            throw e;
        }
    }

    public static void sendHash(final int tagID, final byte[] hash, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws TimeoutException {
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final long[] requests = new long[2];
            requests[0] = prepareToSendInteger(tagID, hash.length, endpoint, scope);
            requests[1] = prepareToSendData(tagID, hash, endpoint, scope);
            awaitRequests(requests, worker, timeoutMs);
        }
    }

    public static void sendAddress(final int tagID, final InetSocketAddress address, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws TimeoutException {
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final byte[] addressBytes = serialize(address);
            final long[] requests = new long[2];
            requests[0] = prepareToSendInteger(tagID, addressBytes.length, endpoint, scope);
            requests[1] = prepareToSendData(tagID, addressBytes, endpoint, scope);
            awaitRequests(requests, worker, timeoutMs);
        }
    }

    public static void sendServerMap(final int tagID, final Map<Integer, InetSocketAddress> serverMap, final Worker worker, final Endpoint endpoint, final int currentServerCount, final int timeoutMs) throws TimeoutException {
        final long[] requests = new long[currentServerCount * 2 + 1];
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            requests[0] = prepareToSendInteger(tagID, currentServerCount, endpoint, scope);

            for (int i = 1; i < currentServerCount * 2 + 1; i += 2) {
                final long[] tmp_requests = prepareToSendAddress(tagID, serverMap.get((i - 2 + 1) / 2), endpoint, scope);
                for (int j = 0; j < 2; j++) {
                    requests[i + j] = tmp_requests[j];
                }
            }
            awaitRequests(requests, worker, timeoutMs);
        }
    }

    private static ByteBuffer receiveData(final int tagID, final int size, final Worker worker, final int timeoutMs, final ResourceScope scope) throws TimeoutException {
        final MemorySegment buffer = MemorySegment.allocateNative(size, scope);
        final long request = worker.receiveTagged(buffer, Tag.of(tagID));
        awaitRequests(new long[]{request}, worker, timeoutMs);
        return buffer.asByteBuffer();
    }

    public static int receiveInteger(final int tagID, final Worker worker, final int timeoutMs, final ResourceScope scope) throws TimeoutException {
        final int number;
        final ByteBuffer integerByteBuffer = receiveData(tagID, Integer.BYTES, worker, timeoutMs, scope);
        number = integerByteBuffer.getInt();
        log.info("[{}] Received \"{}\"", tagID, number);
        return number;
    }

    public static int receiveTagID(final Worker worker, final int timeoutMs, final ResourceScope scope) throws TimeoutException {
        return receiveInteger(0, worker, timeoutMs, scope);
    }

    public static InetSocketAddress receiveAddress(final int tagID, final Worker worker, final int timeoutMs, final ResourceScope scope) throws TimeoutException, SerializationException {
        final InetSocketAddress address;
        final int addressSize = receiveInteger(tagID, worker, timeoutMs, scope);
        final MemorySegment buffer = MemorySegment.allocateNative(addressSize, scope);
        final long request = worker.receiveTagged(buffer, Tag.of(tagID), new RequestParameters(scope));
        awaitRequests(new long[]{request}, worker, timeoutMs);
        address = deserialize(buffer.toArray(ValueLayout.JAVA_BYTE));
        return address;
    }

    public static String receiveKey(final int tagID, final Worker worker, final int timeoutMs, final ResourceScope scope) throws TimeoutException {
        // Get key size in bytes
        final int keySize = receiveInteger(tagID, worker, timeoutMs, scope);

        // Get key as bytes
        final byte[] keyBytes = new byte[keySize];
        final ByteBuffer keyBuffer = receiveData(tagID, keySize, worker, timeoutMs, scope);
        for (int i = 0; i < keySize; i++) {
            keyBytes[i] = keyBuffer.get();
        }
        final String key = new String(keyBytes, StandardCharsets.UTF_8);
        log.info("[{}] Received \"{}\"", tagID, key);

        return key;
    }

    public static String receiveStatusCode(final int tagID, final Worker worker, final int timeoutMs, final ResourceScope scope) throws TimeoutException, SerializationException {
        final String statusCode;
        final ByteBuffer statusCodeByteBuffer = receiveData(tagID, 6, worker, timeoutMs, scope);
        statusCode = String.valueOf(statusCodeByteBuffer.getChar()) + statusCodeByteBuffer.getChar() + statusCodeByteBuffer.getChar();
        log.info("[{}] Received status code: \"{}\"", tagID, statusCode);
        return statusCode;
    }

    public static String receiveOperationName(final int tagID, final Worker worker, final int timeoutMs) throws TimeoutException, SerializationException {
        final String operationName;
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final ByteBuffer statusCodeByteBuffer = receiveData(tagID, 6, worker, timeoutMs, scope);
            operationName = String.valueOf(statusCodeByteBuffer.getChar()) + statusCodeByteBuffer.getChar() + statusCodeByteBuffer.getChar();
        }
        log.info("[{}] Received operation name: \"{}\"", tagID, operationName);
        return operationName;
    }

    public static String awaitPutCompletionSignal(final int tagID, final PlasmaClient plasmaClient, final byte[] id, final Worker worker, final byte[] idToUpdate, final int timeoutMs, final int plasmaTimeoutMs, final ResourceScope scope) throws SerializationException, IOException, ClassNotFoundException {
        final String receivedStatusCode;
        try {
            receivedStatusCode = receiveStatusCode(tagID, worker, timeoutMs, scope);
        } catch (final TimeoutException | SerializationException e) {
            plasmaClient.seal(id);
            deleteById(plasmaClient, id);
            return "401";
        }
        if ("201".equals(receivedStatusCode)) {
            plasmaClient.seal(id);
            if (ObjectUtils.isNotEmpty(idToUpdate)) {
                updateNextIdOfEntry(plasmaClient, idToUpdate, id, plasmaTimeoutMs);
            }
            return "202";
        } else {
            log.error("[{}] Wrong status code: " + receivedStatusCode + " deleting entry.", tagID);
            plasmaClient.seal(id);
            deleteById(plasmaClient, id);
            return "402";
        }
    }

    public static void tearDownEndpoint(final Endpoint endpoint, final Worker worker, final int timeoutMs) {
        try {
            final long[] requests = new long[2];
            requests[0] = endpoint.flush();
            requests[1] = endpoint.closeNonBlocking();
            awaitRequests(requests, worker, timeoutMs);
        } catch (final TimeoutException e) {
            log.warn(e.getMessage());
        }
    }
}
