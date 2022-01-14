package event.listener;

import de.hhu.bsinfo.infinileap.binding.*;
import de.hhu.bsinfo.infinileap.example.util.CommunicationBarrier;
import de.hhu.bsinfo.infinileap.example.util.Requests;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import jdk.incubator.foreign.ValueLayout;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OnMessageEventListenerImpl implements OnMessageEventListener {

    private static final String MESSAGE = "Hello Infinileap!";
    private static final byte[] MESSAGE_BYTES = MESSAGE.getBytes();
    private static final int MESSAGE_SIZE = MESSAGE_BYTES.length;
    private final CommunicationBarrier barrier = new CommunicationBarrier();

    @Override
    public void onMessageEvent(Context context, Worker worker, Endpoint endpoint, ResourceScope scope) throws InterruptedException {

        // Allocate a buffer for receiving the remote's message
        var buffer = MemorySegment.allocateNative(MESSAGE_SIZE, scope);

        // Receive the message
        log.info("Receiving message");

        var request = worker.receiveTagged(buffer, Tag.of(0L), new RequestParameters()
                .setReceiveCallback(barrier::release));

        Requests.await(worker, barrier);
        Requests.release(request);

        log.info("Received \"{}\"", new String(buffer.toArray(ValueLayout.JAVA_BYTE)));
    }
}
