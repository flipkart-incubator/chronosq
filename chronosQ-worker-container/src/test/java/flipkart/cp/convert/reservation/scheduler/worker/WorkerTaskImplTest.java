package flipkart.cp.convert.reservation.scheduler.worker;

import com.codahale.metrics.MetricRegistry;
import flipkart.cp.convert.chronosQ.core.*;
import flipkart.cp.convert.chronosQ.core.*;
import flipkart.cp.convert.reservation.scheduler.worker.task.Factory;
import flipkart.cp.convert.reservation.scheduler.worker.task.WorkerTask;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import flipkart.cp.convert.ha.worker.exception.WorkerException;
import flipkart.cp.convert.reservation.scheduler.worker.task.Factory;
import flipkart.cp.convert.reservation.scheduler.worker.task.WorkerTask;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;

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
    List<SchedulerEntry> values = Arrays.asList(new DefaultSchedulerEntry("something")) ;
    List<String> valueKeys = Arrays.asList("something");
    List<SchedulerEntry> empty = Arrays.asList() ;

    @Before
    public void setUp() throws SchedulerException, InterruptedException {
        MockitoAnnotations.initMocks(this);
        metricRegistry = new MetricRegistry();
        workerTask = new Factory(checkpointer, schedulerStore, timeBucket, schedulerSink, metricRegistry, batchSize).getTask(String.valueOf(partitionNum));
        when(checkpointer.peek(partitionNum)).thenReturn(timerKeyValue);
        when(timeBucket.toBucket(longValue)).thenReturn(longValue);
        when(schedulerStore.get(longValue, partitionNum)).thenReturn(values);
        when(schedulerStore.getNextN(longValue, partitionNum,batchSize)).thenReturn(values).thenReturn(empty);
        when(schedulerSink.giveExpiredListForProcessing(values)).thenReturn(null);
        doNothing().when(schedulerStore).removeBulk(longValue, partitionNum, valueKeys);
        doThrow(new SchedulerException("", ErrorCode.DATASTORE_READWRITE_ERROR)).when(checkpointer).set(timerKeyValue, partitionNum);
    }

    @Test
    public void processTest()  throws Exception {
        Thread thread = new Thread(() -> workerTask.process());
        thread.start();
        Thread.sleep(500);
        workerTask.stopGraceFully();

        verify(checkpointer, atLeastOnce()).peek(partitionNum);
        verify(timeBucket, atLeastOnce()).toBucket(longValue);
        verify(schedulerStore, atLeastOnce()).getNextN(longValue, partitionNum,batchSize);
        verify(schedulerSink, atLeastOnce()).giveExpiredListForProcessing(values);
        verify(schedulerStore, atLeastOnce()).removeBulk(longValue, partitionNum, valueKeys);
        verify(checkpointer, atLeastOnce()).set(timerKeyValue, partitionNum);

        verifyZeroInteractions(checkpointer);
        verifyZeroInteractions(timeBucket);
        verifyZeroInteractions(schedulerStore);
        verifyZeroInteractions(schedulerSink);
    }
}
