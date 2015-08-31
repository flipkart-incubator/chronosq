package flipkart.cp.convert.ha.worker.distribution;

import flipkart.cp.convert.ha.worker.exception.WorkerException;

/**
 * Created by pradeep on 17/02/15.
 */
public interface Restartable {
    void restart() throws WorkerException;
}
