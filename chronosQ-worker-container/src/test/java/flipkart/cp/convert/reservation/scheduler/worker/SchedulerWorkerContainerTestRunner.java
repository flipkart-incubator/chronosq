package flipkart.cp.convert.reservation.scheduler.worker;

import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 04/03/15
 * Time: 2:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class SchedulerWorkerContainerTestRunner {
    public static void main(String[] args) throws SchedulerException {
        Result result = JUnitCore.runClasses(
                WorkerTaskImplTest.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
        }
        System.out.println(result.wasSuccessful());
    }
}
