import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 23/02/15
 * Time: 6:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class SchedulerClientTestRunner {
    public static void main(String[] args) {
        Result result = JUnitCore.runClasses(SchedulerClientTest.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
        }
        System.out.println(result.wasSuccessful());
    }
}
