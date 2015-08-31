package distribution;

import com.codahale.metrics.MetricRegistry;
import flipkart.cp.convert.ha.worker.distribution.DistributionManager;
import flipkart.cp.convert.ha.worker.task.TaskList;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 23/02/15
 * Time: 6:11 PM
 * To change this template use File | Settings | File Templates.
 */


public class DistributionManagerTest {
    private int numOfPartitions = 1;
    private String prefixPath = "/schedulertest/nodes";
    ArrayList<String> expectedTaskIds = new ArrayList<String>();
    private CuratorFramework client;
    private DistributionManager distributionManager;
    private MetricRegistry metricRegistry = new MetricRegistry();
    private String appName = "AppName";
    private String instanceId = "1";
    @MockitoAnnotations.Mock
    private TaskList taskList;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(taskList.getTaskNames()).thenReturn(expectedTaskIds);
        expectedTaskIds.add("0");
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient("localhost:2181", 1000, 5000, retryPolicy);
        client.start();
        distributionManager = new DistributionManager(client, taskList, appName, instanceId, metricRegistry);
    }

    @Test
    public void testNodeCreation() throws Exception {
        assertNotNull(distributionManager);
    }

    @Test
    public void initTest() throws Exception {
        distributionManager.init();
        List<String> taskIds = distributionManager.getTasksToRun();
        assertEquals(taskIds, expectedTaskIds);
        assertEquals(taskIds.size(), distributionManager.getTaskCount());
    }
}
