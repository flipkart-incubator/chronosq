package flipkart.cp.convert.ha.worker.exception;

/**
 * Created by pradeep on 27/02/15.
 */
public class WorkerException extends Exception {

    private ErrorCode errorCode;

    public WorkerException(Throwable ex, ErrorCode errorCode) {
        super(ex);

        this.errorCode = errorCode;
    }
}
