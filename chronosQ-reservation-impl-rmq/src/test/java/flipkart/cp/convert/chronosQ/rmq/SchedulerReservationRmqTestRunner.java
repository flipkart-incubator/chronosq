package flipkart.cp.convert.chronosQ.rmq;

/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 23/02/15
 * Time: 11:45 AM
 * To change this template use File | Settings | File Templates.
 */

import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class SchedulerReservationRmqTestRunner {
    public static void main(String[] args) throws SchedulerException {
        Result result = JUnitCore.runClasses(
                RmqSchedulerSinkTest.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
        }
        System.out.println(result.wasSuccessful());
    }
}
