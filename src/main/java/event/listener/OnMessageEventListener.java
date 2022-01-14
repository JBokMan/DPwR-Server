package event.listener;

import de.hhu.bsinfo.infinileap.binding.Context;
import de.hhu.bsinfo.infinileap.binding.Endpoint;
import de.hhu.bsinfo.infinileap.binding.Worker;
import jdk.incubator.foreign.ResourceScope;

public interface OnMessageEventListener {
    void onMessageEvent(Context context, Worker worker, Endpoint endpoint, ResourceScope scope) throws InterruptedException;
}
