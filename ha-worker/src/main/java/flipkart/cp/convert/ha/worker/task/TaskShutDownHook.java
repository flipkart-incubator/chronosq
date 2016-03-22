package flipkart.cp.convert.ha.worker.task;

import flipkart.cp.convert.ha.worker.exception.WorkerException;

import java.util.List;

/**
 * Created by gupta.rajat on 19/03/16.
 */
public interface TaskShutDownHook {

    void shutDownTasks(List<Runnable> tasks) throws WorkerException;

}
