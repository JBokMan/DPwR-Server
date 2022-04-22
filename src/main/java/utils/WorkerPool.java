package utils;

import de.hhu.bsinfo.infinileap.binding.Context;
import de.hhu.bsinfo.infinileap.binding.ControlException;
import de.hhu.bsinfo.infinileap.binding.Worker;
import de.hhu.bsinfo.infinileap.binding.WorkerParameters;

import java.util.ArrayList;

public class WorkerPool {
    final ArrayList<Worker> pool;
    final WorkerParameters workerParameters;
    int nextWorkerId = 0;

    public WorkerPool(final int count, final WorkerParameters workerParameters, final Context context) throws ControlException {
        this.pool = new ArrayList<>();
        this.workerParameters = workerParameters;
        for (int i = 0; i < count; i++) {
            this.pool.add(context.createWorker(workerParameters));
        }
    }

    public Worker getNextWorker() {
        incrementID();
        return this.pool.get(this.nextWorkerId);
    }

    private void incrementID() {
        this.nextWorkerId = (this.nextWorkerId + 1) % this.pool.size();
    }

    public void resetWorker(final Worker currentWorker, final Context context) {
        final int index = pool.indexOf(currentWorker);
        pool.get(index).close();
        try {
            pool.set(index, context.createWorker(workerParameters));
        } catch (final ControlException e) {
            pool.remove(index);
        }
    }
}
