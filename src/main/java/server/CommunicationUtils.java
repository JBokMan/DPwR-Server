package server;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.CommunicationBarrier;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import de.hhu.bsinfo.infinileap.util.ResourcePool;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import jdk.incubator.foreign.ValueLayout;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

@Slf4j
public class CommunicationUtils {

    public static byte[] getMD5Hash(final String text) {
        try {
            final MessageDigest md = MessageDigest.getInstance("MD5");
            return md.digest(text.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            log.error(e.getMessage());
            return new byte[0];
        }
    }

    public static MemoryDescriptor getMemoryDescriptorOfBytes(final byte[] object, final Context context) {
        final MemorySegment source = MemorySegment.ofArray(object);
        try {
            final MemoryRegion memoryRegion = context.allocateMemory(object.length);
            memoryRegion.segment().copyFrom(source);
            return memoryRegion.descriptor();
        } catch (ControlException e) {
            log.error(e.getMessage());
            return null;
        }
    }

    public static long prepareToSendData(final byte[] data, final Endpoint endpoint, final CommunicationBarrier barrier, final ResourceScope scope) {
        log.info("Prepare to send data");
        final int dataSize = data.length;

        final MemorySegment source = MemorySegment.ofArray(data);
        final MemorySegment buffer = MemorySegment.allocateNative(dataSize, scope);
        buffer.copyFrom(source);

        return endpoint.sendTagged(buffer, Tag.of(0L), new RequestParameters()
                .setSendCallback(barrier::release));
    }

    public static long prepareToSendRemoteKey(final MemoryDescriptor descriptor, final Endpoint endpoint, final CommunicationBarrier barrier) {
        log.info("Prepare to send remote key");
        return endpoint.sendTagged(descriptor, Tag.of(0L), new RequestParameters().setSendCallback(barrier::release));
    }

    public static void sendData(final List<Long> requests, final Worker worker, final CommunicationBarrier barrier) {
        log.info("Sending data");
        try {
            Requests.await(worker, barrier);
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        } finally {
            for (long request : requests) {
                Requests.release(request);
            }
        }
    }

    public static void sendData(final long request, final Worker worker, final CommunicationBarrier barrier) {
        log.info("Sending data");
        try {
            Requests.await(worker, barrier);
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        } finally {
            Requests.release(request);
        }
    }

    public static byte[] receiveData(final int size, final long tagID, final Worker worker, final CommunicationBarrier barrier, final ResourceScope scope) {
        MemorySegment buffer = MemorySegment.allocateNative(size, scope);

        log.info("Receiving message");

        // ToDo this is weird, why does tag matter?
        long request = worker.receiveTagged(buffer, Tag.of(tagID), new RequestParameters()
                .setReceiveCallback(barrier::release));

        try {
            Requests.await(worker, barrier);
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        } finally {
            Requests.release(request);
        }

        return buffer.toArray(ValueLayout.JAVA_BYTE);
    }

    public static MemoryDescriptor receiveMemoryDescriptor(final long tagID, final Worker worker, final CommunicationBarrier barrier) {
        var descriptor = new MemoryDescriptor();

        log.info("Receiving Remote Key");

        var request = worker.receiveTagged(descriptor, Tag.of(tagID), new RequestParameters()
                .setReceiveCallback(barrier::release));

        try {
            Requests.await(worker, barrier);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Requests.release(request);

        return descriptor;
    }

    public static byte[] receiveRemoteObject(final MemoryDescriptor descriptor, final Endpoint endpoint, final Worker worker, final CommunicationBarrier barrier, final ResourceScope scope, final ResourcePool resourcePool) {
        RemoteKey remoteKey = null;
        try {
            remoteKey = endpoint.unpack(descriptor);
        } catch (ControlException e) {
            e.printStackTrace();
        }
        if (remoteKey == null) {
            log.error("Remote key was null");
            return new byte[0];
        }
        var targetBuffer = MemorySegment.allocateNative(descriptor.remoteSize(), scope);
        resourcePool.push(remoteKey);

        long request = endpoint.get(targetBuffer, descriptor.remoteAddress(), remoteKey, new RequestParameters()
                .setReceiveCallback(barrier::release));

        try {
            Requests.await(worker, barrier);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Requests.release(request);

        return targetBuffer.toArray(ValueLayout.JAVA_BYTE);
    }

    public static byte[] serializeObject(final Object object) {
        try {
            return SerializationUtils.serialize((Serializable) object);
        } catch (Exception e) {
            log.warn(e.getMessage());
            return new byte[0];
        }
    }
}
