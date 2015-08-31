package flipkart.cp.convert.chronosQ.exceptions;

/**
 * Created by pradeep on 05/02/15.
 */
public class SchedulerException extends Exception {
    private ErrorCode errorCode;

    public SchedulerException() {

    }

    public SchedulerException(ErrorCode errorCode) {
        this.errorCode = errorCode;
    }

    public SchedulerException(String message, ErrorCode errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public SchedulerException(String message, Throwable cause, ErrorCode errorCode) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public SchedulerException(Throwable cause, ErrorCode errorCode) {
        super(cause);
        this.errorCode = errorCode;
    }

}
