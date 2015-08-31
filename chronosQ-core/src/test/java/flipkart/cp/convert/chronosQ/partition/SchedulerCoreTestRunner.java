package flipkart.cp.convert.chronosQ.partition;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import flipkart.cp.convert.chronosQ.partition.partition.MurmurHashPartitionerTest;
import flipkart.cp.convert.chronosQ.partition.timebucket.SecondGroupedTimeBucketTest;

/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 23/02/15
 * Time: 6:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class SchedulerCoreTestRunner {
    public static void main(String[] args) {
        Result result = JUnitCore.runClasses(MurmurHashPartitionerTest.class, SecondGroupedTimeBucketTest.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
        }
        System.out.println(result.wasSuccessful());
    }
}
