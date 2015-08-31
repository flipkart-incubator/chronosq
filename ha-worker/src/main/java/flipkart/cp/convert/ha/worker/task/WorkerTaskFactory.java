package flipkart.cp.convert.ha.worker.task;

/**
 * Created by pradeep on 16/02/15.
 */
public interface WorkerTaskFactory {

    Runnable getTask(String taskName);

}
