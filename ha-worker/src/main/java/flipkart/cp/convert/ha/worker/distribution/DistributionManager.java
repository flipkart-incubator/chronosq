package flipkart.cp.convert.ha.worker.distribution;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import flipkart.cp.convert.ha.worker.exception.ErrorCode;
import flipkart.cp.convert.ha.worker.exception.WorkerException;
import flipkart.cp.convert.ha.worker.task.TaskList;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by pradeep on 16/02/15.
 */
public class DistributionManager implements Closeable, TreeCacheListener {

    private static final String PREFIX_PATH = "/scheduler";

    private final PersistentEphemeralNode myNode;
    private final String zkPrefix;
    private TaskList taskList;
    private final String instanceId;
    private final CuratorFramework client;
    private final String appName;
    private List<String> tasksToRun;
    private List<String> workerInstances;
    private Restartable restartable;
    private MetricRegistry metricRegistry;
    private static Logger log = LoggerFactory.getLogger(DistributionManager.class.getName());

    public DistributionManager(CuratorFramework client, TaskList taskList, String appName, String instanceId, MetricRegistry metricRegistry) throws WorkerException {
        this.taskList = taskList;
        this.client = client;
        this.appName = appName;
        this.instanceId = instanceId;
        this.metricRegistry = metricRegistry;
        this.zkPrefix = PREFIX_PATH + "/" + appName;
        this.myNode = new PersistentEphemeralNode(client, PersistentEphemeralNode.Mode.EPHEMERAL, zkPrefix + "/" + this.instanceId, this.instanceId.getBytes());
        myNode.start();
        try {
            myNode.waitForInitialCreate(3, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            log.error("Exception occurred :" + ex.fillInStackTrace());
            throw new WorkerException(ex, ErrorCode.WORKER_RUNTIME_ERROR);
        }
        log.info("Ephemral node created " + instanceId);
        _attachListener();
        metricRegistry.register(MetricRegistry.name(DistributionManager.class, appName,"WorkerInstances", "count"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return workerInstances.size();
                    }
                });
    }

    private void _attachListener() throws WorkerException {
        try {
            TreeCache treeCache = new TreeCache(client, zkPrefix);
            treeCache.getListenable().addListener(this);
            treeCache.start();
            log.info("Listener attached for " + instanceId);
        } catch (Exception ex) {
            log.error("Exception occurred :" + ex.fillInStackTrace());
            throw new WorkerException(ex, ErrorCode.WORKER_RUNTIME_ERROR);
        }

    }

    public void init() throws WorkerException {
        this.workerInstances = getWorkerInstances(zkPrefix);
        int instanceIndex = -1;
        for (int i = 0; i < workerInstances.size(); i++) {
            String instanceName = workerInstances.get(i);
            if (instanceName.equals(this.instanceId)) {
                instanceIndex = i;
                break;
            }
        }
        if (instanceIndex < 0)
            throw new IllegalStateException("'" + instanceId + "' instanceId is unknown & not configured!");
        tasksToRun = _createTaskIdsForExecution(workerInstances, taskList, instanceIndex);
        log.info("Init over for " + instanceId);
    }

    private List<String> getWorkerInstances(String prefixPath) throws WorkerException {
        try {
            List<String> workerInstances = client.getChildren().forPath(prefixPath);
            log.info("Worker instances for " + prefixPath + "are " + workerInstances);
            return workerInstances;
        } catch (Exception ex) {
            log.error("Exception occurred :" + ex.fillInStackTrace());
            throw new WorkerException(ex, ErrorCode.WORKER_RUNTIME_ERROR);
        }
    }

    public int getTaskCount() {
        return tasksToRun.size();
    }

    public List<String> getTasksToRun() {
        return tasksToRun;
    }

    public void setRestartable(Restartable restartable) {
        this.restartable = restartable;
    }

    @Override
    public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
        log.info("Event raised for " + instanceId + "Type -" + treeCacheEvent.getType());
        List<String> paths = curatorFramework.getChildren().forPath(zkPrefix);
        List<String> values = new LinkedList<String>();
        for (String path : paths) {
            String instanceIdVal = getInstanceIdFromPath(path, zkPrefix);
            values.add(instanceIdVal);
        }
        if (hasHostListChanged(values)) {
            restartable.restart();
        }
    }

    private boolean hasHostListChanged(List<String> values) {
        String newHash = createHash(values);
        String oldHash = createHash(workerInstances);
        return (!newHash.equals(oldHash));
    }

    @Override
    public void close() {
    }

    private static String getInstanceIdFromPath(String path, String prefixPath) {
        return path.replaceFirst(prefixPath + "/", "");
    }

    private static String createHash(List<String> strings) {
        StringBuilder sb = new StringBuilder();
        for (String s : strings) {
            sb.append(s);
            sb.append(":");
        }
        return sb.toString();
    }

    private static List<String> _createTaskIdsForExecution(List<String> workerInstances, TaskList taskList, int instanceIndex) {
        List<String> instances = new LinkedList<String>();
        ArrayList<String> taskNames = taskList.getTaskNames();
        for (int i = 0; i < taskNames.size(); i++) {
            if (((i) % workerInstances.size()) == instanceIndex) {
                instances.add(taskNames.get(i));
            }
        }
        log.info("TaskId's for Execution generated by " + instanceIndex + "is " + instances);

        return instances;
    }
}
