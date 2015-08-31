import com.codahale.metrics.MetricRegistry;
import flipkart.cp.convert.chronosQ.client.SchedulerClient;
import flipkart.cp.convert.chronosQ.core.Partitioner;
import flipkart.cp.convert.chronosQ.core.SchedulerStore;
import flipkart.cp.convert.chronosQ.core.TimeBucket;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;
import static org.junit.Assert.assertTrue;

/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 23/02/15
 * Time: 6:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class SchedulerClientTest {
    @Mock
    private SchedulerStore store;
    @Mock
    private TimeBucket timeBucket;
    @Mock
    private Partitioner partitioner;

    private MetricRegistry metricRegistry=new MetricRegistry();
    private SchedulerClient<Entry> schedulerClient;
    @Mock
    private Entry entry = new Entry();
    private long timeValue = 100L;
    private int partitionNum = 1;
    private long newTimeValue = 120L;
    private long dataStoreReturnValue = 1;
    private long dataStoreWrongValue = 0;


    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        schedulerClient = new SchedulerClient.Builder<Entry>()
                .withStore(store)
                .withTimeBucket(timeBucket)
                .withPartitioner(partitioner)
                .withMetricRegistry(metricRegistry)
                .buildOrGet();
        when(partitioner.getPartition(null)).thenReturn(partitionNum);
        when(timeBucket.toBucket(timeValue)).thenReturn(timeValue);
        when(timeBucket.toBucket(newTimeValue)).thenReturn(newTimeValue);
        doNothing().when(store).add(null, timeValue, partitionNum);
        when(store.update(null, timeValue, newTimeValue, partitionNum)).thenReturn(dataStoreReturnValue);
        when(store.remove(null, timeValue, partitionNum)).thenReturn(dataStoreReturnValue);
    }

    @Test
    public void testAdd() throws SchedulerException {
        schedulerClient.add(entry, timeValue);
        verify(store).add(null, timeValue, partitionNum);
        verifyForCorrectFlow();
    }

    @Test
    public void testUpdate() throws SchedulerException {
        boolean result = schedulerClient.update(entry, timeValue, newTimeValue);
        assertTrue(result);
        verify(timeBucket).toBucket(newTimeValue);
        verify(store).update(null, timeValue, newTimeValue, partitionNum);
        verifyForCorrectFlow();
    }

    @Test
    public void testUpdateWrongValue() throws SchedulerException {
        when(store.update(null, timeValue, newTimeValue, partitionNum)).thenReturn(dataStoreWrongValue);
        boolean result = schedulerClient.update(entry, timeValue, newTimeValue);
        assertFalse(result);
        verify(timeBucket).toBucket(newTimeValue);
        verify(store).update(null, timeValue, newTimeValue, partitionNum);
        verifyForCorrectFlow();
    }

    @Test
    public void testRemove() throws SchedulerException {
        boolean result = schedulerClient.remove(entry, timeValue);
        assertTrue(result);
        verify(store).remove(null, timeValue, partitionNum);
        verifyForCorrectFlow();
    }

    @Test
    public void testRemoveWrongValue() throws SchedulerException {
        when(store.remove(null, timeValue, partitionNum)).thenReturn(dataStoreWrongValue);
        boolean result = schedulerClient.remove(entry, timeValue);
        assertFalse(result);
        verify(store).remove(null, timeValue, partitionNum);
        verifyForCorrectFlow();
    }

    @Test(expected = SchedulerException.class)
    public void testAddException() throws SchedulerException {
        doThrow(new SchedulerException("Store Error", ErrorCode.DATASTORE_READWRITE_ERROR)).when(store).add(null, timeValue, partitionNum);
        schedulerClient.add(entry, timeValue);
        verify(store).add(null, timeValue, partitionNum);
        verifyForCorrectFlow();
    }

    @Test(expected = SchedulerException.class)
    public void testUpdateException() throws SchedulerException {
        doThrow(new SchedulerException("Store Error", ErrorCode.DATASTORE_READWRITE_ERROR)).when(store).update(null, timeValue, newTimeValue, partitionNum);
        schedulerClient.update(entry, timeValue, newTimeValue);
        verify(store).update(null, timeValue, newTimeValue, partitionNum);
        verifyForCorrectFlow();
    }

    @Test(expected = SchedulerException.class)
    public void testRemoveException() throws SchedulerException {
        doThrow(new SchedulerException("Store Error", ErrorCode.DATASTORE_READWRITE_ERROR)).when(store).remove(null, timeValue, partitionNum);
        schedulerClient.remove(entry, timeValue);
        verify(store).remove(null, timeValue, partitionNum);
        verifyForCorrectFlow();
    }

    private void verifyForCorrectFlow() throws SchedulerException {
        verify(partitioner).getPartition(null);
        verify(timeBucket).toBucket(timeValue);
        verify(entry).getStringValue();
        verifyZeroInteractions(entry);
        verifyZeroInteractions(partitioner);
        verifyZeroInteractions(timeBucket);
        verifyZeroInteractions(store);
    }
}