package flipkart.cp.convert.reservation.scheduler.worker;

import com.codahale.metrics.MetricRegistry;
import flipkart.cp.convert.reservation.scheduler.worker.task.Factory;
import flipkart.cp.convert.reservation.scheduler.worker.task.WorkerTask;
import flipkart.cp.convert.chronosQ.core.SchedulerCheckpointer;
import flipkart.cp.convert.chronosQ.core.SchedulerSink;
import flipkart.cp.convert.chronosQ.core.SchedulerStore;
import flipkart.cp.convert.chronosQ.core.TimeBucket;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

import java.util.Date;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 23/02/15
 * Time: 6:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class WorkerTaskImplTest {
    @Mock
    private SchedulerCheckpointer checkpointer;
    @Mock
    private SchedulerStore schedulerStore;
    @Mock
    private TimeBucket timeBucket;
    @Mock
    private SchedulerSink schedulerSink;
    private MetricRegistry metricRegistry;
    private final int partitionNum = 1;
    private WorkerTask workerTask;
    private final int batchSize = 10;
    private String timerKeyValue = String.valueOf(new Date().getTime() - 10000);
    private long longValue = Long.valueOf(timerKeyValue);
    List<String> values = null;

    @Before
    public void setUp() throws SchedulerException, InterruptedException {
        MockitoAnnotations.initMocks(this);
        metricRegistry = new MetricRegistry();
        workerTask = new Factory(checkpointer, schedulerStore, timeBucket, schedulerSink, metricRegistry, batchSize).getTask(String.valueOf(partitionNum));
        when(checkpointer.peek(partitionNum)).thenReturn(timerKeyValue);
        when(timeBucket.toBucket(longValue)).thenReturn(longValue);
        when(schedulerStore.get(longValue, partitionNum)).thenReturn(null);
        doNothing().when(schedulerSink).giveExpiredListForProcessing(values);
        doNothing().when(schedulerStore).removeBulk(longValue, partitionNum, values);
        doThrow(new SchedulerException("", ErrorCode.DATASTORE_READWRITE_ERROR)).when(checkpointer).set(timerKeyValue, partitionNum);
    }

    @Test
    public void processTest() throws SchedulerException {
        workerTask.process();
        verify(checkpointer).peek(partitionNum);
        verify(timeBucket).toBucket(longValue);
        verify(schedulerStore).get(longValue, partitionNum);
        verify(schedulerSink).giveExpiredListForProcessing(values);
        verify(schedulerStore).removeBulk(longValue, partitionNum, values);
        verify(checkpointer).set(timerKeyValue, partitionNum);
        verifyZeroInteractions(checkpointer);
        verifyZeroInteractions(timeBucket);
        verifyZeroInteractions(schedulerStore);
        verifyZeroInteractions(schedulerSink);
    }
}
