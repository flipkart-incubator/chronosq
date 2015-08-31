package flipkart.cp.convert.chronosQ.partition.partition;

import flipkart.cp.convert.chronosQ.core.Partitioner;
import flipkart.cp.convert.chronosQ.core.impl.MurmurHashPartioner;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 23/02/15
 * Time: 6:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class MurmurHashPartitionerTest {

    private Partitioner paritioner;
    private String value = "testvalue";

    @Before
    public void setUp() {
        paritioner = new MurmurHashPartioner(5);
    }

    @Test
    public void testHash() throws SchedulerException {
        int value1 = paritioner.getPartition(value);
        int value2 = paritioner.getPartition(value);
        assertFalse(value1 < 0 || value2 < 0);
        assertEquals(value1, value2);
    }

    @Test(expected = SchedulerException.class)
    public void testHashException() throws SchedulerException {
        paritioner.getPartition(null);
    }
}