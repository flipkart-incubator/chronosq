package flipkart.cp.convert.chronosQ.partition.timebucket;

/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 23/02/15
 * Time: 11:56 AM
 * To change this template use File | Settings | File Templates.
 */

import flipkart.cp.convert.chronosQ.core.TimeBucket;
import flipkart.cp.convert.chronosQ.core.impl.SecondGroupedTimeBucket;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SecondGroupedTimeBucketTest {
    private TimeBucket secondGroupedTimeBucket;
    private final int timeStampBucketNameSame = 100000;
    private final int nextBucketNameValue = 120000;
    private final int prevBucketName = 80000;
    private final int prevBucketTimeStamp = 95000;

    @Before
    public void SetUp() {
        secondGroupedTimeBucket = new SecondGroupedTimeBucket(20);
    }

    @Test
    public void testToBucketConversionWhenTimeStampGiven() {
        testTimeStampBucketNameSame();
        long bucketName = secondGroupedTimeBucket.toBucket(prevBucketTimeStamp);
        assertEquals(bucketName, prevBucketName);
    }

    @Test
    public void testToBucketConversionWhenBucketNameGiven() {
        testTimeStampBucketNameSame();
        long bucketValue = secondGroupedTimeBucket.toBucket(prevBucketName);
        assertEquals(bucketValue, prevBucketName);
    }

    @Test
    public void testNextBucketConversionWhenTimeStampGiven() {
        testNextTimeStampBucketNameSame();
        long nextBucketName = secondGroupedTimeBucket.next(prevBucketTimeStamp);
        assertEquals(nextBucketName, timeStampBucketNameSame);
    }

    @Test
    public void testNextBucketConversionWhenBucketNameGiven() {
        testNextTimeStampBucketNameSame();
        long nextBucketName = secondGroupedTimeBucket.next(prevBucketName);
        assertEquals(nextBucketName, timeStampBucketNameSame);
    }

    private void testTimeStampBucketNameSame() {
        long bucketValue = secondGroupedTimeBucket.toBucket(timeStampBucketNameSame);
        assertEquals(bucketValue, timeStampBucketNameSame);
    }

    private void testNextTimeStampBucketNameSame() {
        long nextBucketName = secondGroupedTimeBucket.next(timeStampBucketNameSame);
        assertEquals(nextBucketName, nextBucketNameValue);
    }

}