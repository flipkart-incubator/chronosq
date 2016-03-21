package flipkart.cp.convert.ha.worker.distribution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import flipkart.cp.convert.ha.worker.exception.WorkerException;
import flipkart.cp.convert.ha.worker.task.TaskShutDownHook;
import flipkart.cp.convert.ha.worker.task.WorkerTaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WorkerManager implements Restartable {
    private final WorkerTaskFactory workerTaskFactory;
    private final DistributionManager distributionManager;
    @VisibleForTesting
    private ExecutorService executorService;
    private List<Runnable> taskList;
    static Logger log = LoggerFactory.getLogger(WorkerManager.class.getName());

    public WorkerManager(WorkerTaskFactory workerTaskFactory, DistributionManager distributionManager) {
        this.workerTaskFactory = workerTaskFactory;
        this.distributionManager = distributionManager;
        this.distributionManager.setRestartable(this);
    }

    public synchronized void start() throws WorkerException {
        this.distributionManager.init();
        if (distributionManager.getTaskCount() > 0) {
            executorService = Executors.newFixedThreadPool(distributionManager.getTaskCount());
            List<String> taskIds = distributionManager.getTasksToRun();
            taskList = new ArrayList<>();
            log.info("Task ids " + taskIds);
            for (String taskId : taskIds) {
                log.info("Starting Thread for " + taskId);
                Runnable task = workerTaskFactory.getTask(taskId);
                executorService.submit(task);
                taskList.add(task);
            }
        }
    }

    private synchronized void _stopExecutorsIdempotent() throws WorkerException {
        if (executorService != null && !executorService.isShutdown()) {
            log.info("Shutting down at " + new Date());
            Optional<TaskShutDownHook> shutDownHook = distributionManager.getTaskShutDownHook();
            if (shutDownHook.isPresent()) {
                shutDownHook.get().shutDownTasks(taskList);
            }
            executorService.shutdownNow();
        }
    }

    public void stop() throws WorkerException {
        _stopExecutorsIdempotent();
    }

    @Override
    public void restart() throws WorkerException {
        log.info("Restarting scheduler worker ");
        stop();
        start();
    }
}
