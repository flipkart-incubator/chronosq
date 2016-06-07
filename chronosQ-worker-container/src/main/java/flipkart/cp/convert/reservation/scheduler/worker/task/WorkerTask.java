package flipkart.cp.convert.reservation.scheduler.worker.task;

import flipkart.cp.convert.ha.worker.task.StoppableTask;

/**
 * Created by pradeep on 16/02/15.
 */
public abstract class WorkerTask implements StoppableTask {

    private final Integer partitionNum;

    protected WorkerTask(String taskName) {
        this.partitionNum = Integer.valueOf(taskName);
    }

    protected Integer getPartitionNum() {
        return partitionNum;
    }

    @Override
    public final void run() {
        //TODO
        process();
    }

    public abstract void process();

}
