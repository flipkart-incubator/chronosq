import flipkart.cp.convert.ha.worker.distribution.WorkerManager;
import flipkart.cp.convert.ha.worker.exception.ErrorCode;
import flipkart.cp.convert.ha.worker.exception.WorkerException;
import flipkart.cp.convert.ha.worker.distribution.DistributionManager;

import flipkart.cp.convert.ha.worker.task.WorkerTaskFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 23/02/15
 * Time: 6:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class WorkerManagerTest {
    @Mock
    private WorkerTaskFactory workerTaskFactory;
    @Mock
    private DistributionManager distributionManager;

    @Mock
    private Runnable runnable;
    private WorkerManager workerManager;
    private int noOfThreads = 1;
    private String taskIdValue = "1";
    private List<String> taskIds = Arrays.asList(taskIdValue);


    @Before
    public void setUp() throws WorkerException {
        MockitoAnnotations.initMocks(this);
        workerManager = new WorkerManager(workerTaskFactory, distributionManager);
        doNothing().when(distributionManager).setRestartable(workerManager);
        doNothing().when(distributionManager).init();
        when(distributionManager.getTaskCount()).thenReturn(noOfThreads);
        when(distributionManager.getTasksToRun()).thenReturn(taskIds);
        when(workerTaskFactory.getTask(taskIdValue)).thenReturn(runnable);
    }

    @Test
    public void testStart() throws WorkerException {
        workerManager.start();
        testStartFlow();
    }


    @Test
    public void testRestart() throws WorkerException {
        workerManager.restart();
        testStartFlow();
    }

    private void testStartFlow() throws WorkerException {
        verify(distributionManager).getTaskCount();
        verify(distributionManager).getTasksToRun();
        verify(workerTaskFactory).getTask(taskIdValue);
        verify(distributionManager).setRestartable(workerManager);
        testInit();
    }

    private void testInit() throws WorkerException {
        verify(distributionManager).init();
        verifyZeroInteractions(distributionManager);
        verifyZeroInteractions(workerTaskFactory);
    }

}


