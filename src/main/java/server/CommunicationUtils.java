package server;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.CommunicationBarrier;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import jdk.incubator.foreign.ValueLayout;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

@Slf4j
public class CommunicationUtils {

    public static byte[] getMD5Hash(final String text) throws NoSuchAlgorithmException {
        final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        return messageDigest.digest(text.getBytes(StandardCharsets.UTF_8));
    }

    public static MemoryDescriptor getMemoryDescriptorOfBytes(final byte[] object, final Context context) throws ControlException {
        final MemorySegment source = MemorySegment.ofArray(object);
        final MemoryRegion memoryRegion = context.allocateMemory(object.length);
        memoryRegion.segment().copyFrom(source);
        return memoryRegion.descriptor();
    }

    public static Long prepareToSendData(final byte[] data, final long tagID, final Endpoint endpoint, final ResourceScope scope) {
        if (log.isInfoEnabled()) {
            log.info("Prepare to send data");
        }
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
        if (log.isInfoEnabled()) {
            log.info("Sending data");
        }
        for (final Long request : requests) {
            Requests.poll(worker, request);
        }
    }

    public static byte[] receiveData(final int size, final long tagID, final Worker worker, final ResourceScope scope) {
        final CommunicationBarrier barrier = new CommunicationBarrier();
        final MemorySegment buffer = MemorySegment.allocateNative(size, scope);

        if (log.isInfoEnabled()) {
            log.info("Receiving message");
        }

        final long request = worker.receiveTagged(buffer, Tag.of(tagID), new RequestParameters()
                .setReceiveCallback(barrier::release));

        awaitRequestIfNecessary(request, worker, barrier);

        return buffer.toArray(ValueLayout.JAVA_BYTE);
    }

    public static MemoryDescriptor receiveMemoryDescriptor(final long tagID, final Worker worker) {
        final CommunicationBarrier barrier = new CommunicationBarrier();
        final MemoryDescriptor descriptor = new MemoryDescriptor();

        if (log.isInfoEnabled()) {
            log.info("Receiving Remote Key");
        }

        final long request = worker.receiveTagged(descriptor, Tag.of(tagID), new RequestParameters()
                .setReceiveCallback(barrier::release));

        awaitRequestIfNecessary(request, worker, barrier);

        return descriptor;
    }

    @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
    public static byte[] receiveRemoteObject(final MemoryDescriptor descriptor, final Endpoint endpoint, final Worker worker, final ResourceScope scope, final ResourcePool resourcePool) throws ControlException {
        final CommunicationBarrier barrier = new CommunicationBarrier();
        RemoteKey remoteKey;

        remoteKey = endpoint.unpack(descriptor);

        final MemorySegment targetBuffer = MemorySegment.allocateNative(descriptor.remoteSize(), scope);
        resourcePool.push(remoteKey);

        final long request = endpoint.get(targetBuffer, descriptor.remoteAddress(), remoteKey, new RequestParameters()
                .setReceiveCallback(barrier::release));

        awaitRequestIfNecessary(request, worker, barrier);

        return targetBuffer.toArray(ValueLayout.JAVA_BYTE);
    }

    private static void awaitRequestIfNecessary(final long request, final Worker worker, final CommunicationBarrier barrier) {
        if (!Status.isError(request)) {
            if (log.isWarnEnabled()) {
                log.warn("A request has an error status");
            }
        }
        if (!Status.is(request, Status.OK)) {
            try {
                Requests.await(worker, barrier);
            } catch (InterruptedException e) {
                if (log.isErrorEnabled()) {
                    log.error(e.getMessage());
                }
            } finally {
                Requests.release(request);
            }
        }
    }

    public static byte[] serializeObject(final Object object) throws SerializationException {
        return SerializationUtils.serialize((Serializable) object);
    }
}
