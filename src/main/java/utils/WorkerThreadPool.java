package utils;

import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.binding.WorkerParameters;
import server.WorkerThread;

import java.util.ArrayList;

public class WorkerThreadPool {
    final ArrayList<WorkerThread> pool;
    int nextWorkerId = 0;

    public WorkerThreadPool(final int count, final WorkerParameters workerParameters) throws ControlException {
        this.pool = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            this.pool.add(new WorkerThread(workerParameters));
            this.pool.get(i).start();
        }
    }

    public WorkerThread getNextWorkerThread() {
        incrementID();
        return this.pool.get(this.nextWorkerId);
    }

    private void incrementID() {
        this.nextWorkerId = (this.nextWorkerId + 1) % this.pool.size();
    }

    public void close() {
        for (final WorkerThread workerThread : pool) {
            workerThread.close();
        }
    }
}
