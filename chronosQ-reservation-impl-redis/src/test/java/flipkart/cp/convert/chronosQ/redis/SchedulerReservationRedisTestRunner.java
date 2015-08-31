package flipkart.cp.convert.chronosQ.redis;

import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 03/03/15
 * Time: 12:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class SchedulerReservationRedisTestRunner {
    public static void main(String[] args) throws SchedulerException {
        Result result = JUnitCore.runClasses(
                RedisSchedulerCheckPointTest.class,
                RedisSchedulerStoreTest.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
        }
        System.out.println(result.wasSuccessful());
    }
}
