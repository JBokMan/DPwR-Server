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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
        log.info("Prepare to send data");
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

    private static ArrayList<Long> prepareToSendAddress(final int tagID, final InetSocketAddress address, final Endpoint endpoint, final ResourceScope scope) {
        final ArrayList<Long> requests = new ArrayList<>();
        final byte[] addressBytes = serialize(address);
        requests.add(prepareToSendInteger(tagID, addressBytes.length, endpoint, scope));
        requests.add(prepareToSendData(tagID, addressBytes, endpoint, scope));
        return requests;
    }

    private static Long prepareToSendRemoteKey(final int tagID, final MemoryDescriptor descriptor, final Endpoint endpoint, final ResourceScope scope) {
        log.info("Prepare to send remote key");
        return endpoint.sendTagged(descriptor, Tag.of(tagID), new RequestParameters(scope));
    }

    private static void awaitRequest(final long request, final Worker worker, final int timeoutMs) throws TimeoutException, InterruptedException {
        if (log.isInfoEnabled()) {
            log.info("Await request");
        }
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

    public static void awaitRequests(final List<Long> requests, final Worker worker, final int timeoutMs) throws TimeoutException {
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
                worker.cancelRequest(request);
            }
        }
        if (timeoutHappened) {
            log.error("A timeout occurred while sending data");
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
        log.info("Send object address");
        MemoryDescriptor objectAddress = null;
        try {
            objectAddress = getMemoryDescriptorOfByteBuffer(objectBuffer, context);
        } catch (final UnsupportedOperationException e) {
            log.error(e.getMessage());
        }
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final Long request = prepareToSendRemoteKey(tagID, objectAddress, endpoint, scope);
            awaitRequests(List.of(request), worker, timeoutMs);
        }
    }

    public static void sendObjectAddress(final int tagID, final byte[] object, final Endpoint endpoint, final Worker worker, final Context context, final int timeoutMs) throws CloseException, ControlException, TimeoutException {
        log.info("Send object address");
        MemoryDescriptor objectAddress = null;
        try {
            objectAddress = getMemoryDescriptorOfByteArray(object, context);
        } catch (final UnsupportedOperationException e) {
            log.error(e.getMessage());
        }
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final Long request = prepareToSendRemoteKey(tagID, objectAddress, endpoint, scope);
            awaitRequests(List.of(request), worker, timeoutMs);
        }
    }

    public static void createEntryAndSendNewEntryAddress(final int tagID, final PlasmaClient plasmaClient, final byte[] id, final int entrySize, final Endpoint endpoint, final Worker worker, final Context context, final int timeoutMs) throws TimeoutException, ControlException, CloseException {
        // create new plasma entry with correct id and size and send its memory address to client
        final ByteBuffer byteBuffer = plasmaClient.create(id, entrySize, new byte[0]);
        try {
            sendStatusCode(tagID, "200", endpoint, worker, timeoutMs);
            sendObjectAddress(tagID, byteBuffer, endpoint, worker, context, timeoutMs);
        } catch (final TimeoutException e) {
            plasmaClient.seal(id);
            deleteById(plasmaClient, id);
            throw e;
        }
    }

    public static void sendHash(final int tagID, final byte[] hash, final Endpoint endpoint, final Worker worker, final int timeoutMs) throws TimeoutException {
        try (final ResourceScope scope = ResourceScope.newConfinedScope()) {
            final ArrayList<Long> requests = new ArrayList<>();
            requests.add(prepareToSendInteger(tagID, hash.length, endpoint, scope));
            requests.add(prepareToSendData(tagID, hash, endpoint, scope));
            awaitRequests(requests, worker, timeoutMs);
        }
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
            requests.add(prepareToSendInteger(tagID, currentServerCount, endpoint, scope));
            for (int i = 0; i < currentServerCount; i++) {
                requests.addAll(prepareToSendAddress(tagID, serverMap.get(i), endpoint, scope));
            }
            awaitRequests(requests, worker, timeoutMs);
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

    public static String awaitPutCompletionSignal(final int tagID, final PlasmaClient plasmaClient, final byte[] id, final Worker worker, final byte[] idToUpdate, final int timeoutMs, final int plasmaTimeoutMs) throws SerializationException {
        final String receivedStatusCode;
        try {
            receivedStatusCode = receiveStatusCode(tagID, worker, timeoutMs);
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
            log.error("Wrong status code: " + receivedStatusCode + " deleting entry.");
            plasmaClient.seal(id);
            deleteById(plasmaClient, id);
            return "402";
        }
    }

    public static void tearDownEndpoint(final Endpoint endpoint, final Worker worker, final int timeoutMs) {
        try {
            final ArrayList<Long> requests = new ArrayList<>();
            requests.add(endpoint.flush());
            requests.add(endpoint.closeNonBlocking());
            awaitRequests(requests, worker, timeoutMs);
        } catch (final TimeoutException e) {
            log.warn(e.getMessage());
        }
    }
}
