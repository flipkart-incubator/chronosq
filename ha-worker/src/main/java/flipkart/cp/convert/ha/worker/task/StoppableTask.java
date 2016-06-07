package flipkart.cp.convert.ha.worker.task;

import flipkart.cp.convert.ha.worker.exception.WorkerException;

/**
 * Created by gupta.rajat on 22/03/16.
 */
public interface StoppableTask extends Runnable {

    void stopGraceFully() throws WorkerException;

    void stopPoisonPill();
}
