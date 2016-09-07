package flipkart.cp.convert.ha.worker;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import flipkart.cp.convert.ha.worker.di.WorkerModule;
import flipkart.cp.convert.ha.worker.distribution.DistributionManager;
import flipkart.cp.convert.ha.worker.distribution.WorkerManager;
import flipkart.cp.convert.ha.worker.exception.ErrorCode;
import flipkart.cp.convert.ha.worker.exception.WorkerException;
import flipkart.cp.convert.ha.worker.task.TaskList;
import flipkart.cp.convert.ha.worker.task.WorkerTaskFactory;
import org.apache.commons.cli.*;
import org.apache.curator.framework.CuratorFramework;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by pradeep on 18/02/15.
 */
public class Bootstrap {

    private static String DEFAULT_NAMESPACE = "flipkart";

    private static Logger log = LoggerFactory.getLogger(Bootstrap.class.getName());

    private final Lock lock = new ReentrantLock();

    private final String instanceId;
    private final String nameSpace;
    private final String hostName;
    private String listenerPathSuffix = "";
    Reflections reflections;

    public Bootstrap(String instanceId, String nameSpace, String hostName) {
        this.instanceId = instanceId;
        this.nameSpace = nameSpace;
        this.hostName = hostName;
    }

    public void start() throws WorkerException {
        List<AbstractModule> moduleList = _getGuiceModules(hostName);
        List<WorkerManager> workerManagerList = new ArrayList<WorkerManager>();
        for (AbstractModule module : moduleList) {
            WorkerModule annotation = module.getClass().getAnnotation(WorkerModule.class);
            try {
                workerManagerList.add(startApp(annotation.appName(), Guice.createInjector(module)));
            } catch (WorkerException e) {
                throw new WorkerException(e, ErrorCode.WORKER_RUNTIME_ERROR);
            }
        }
        System.out.println("Worker has been started");
        Runtime.getRuntime().addShutdownHook(new ShutdownThread(workerManagerList));
        awaitShutdown();
    }

    private List<AbstractModule> _getGuiceModules(String hostName) {
        List<AbstractModule> moduleList = new ArrayList<AbstractModule>();
        reflections = new Reflections(this.nameSpace != null ? this.nameSpace : DEFAULT_NAMESPACE);
        Set<Class<? extends AbstractModule>> allGuiceModules = reflections.getSubTypesOf(AbstractModule.class);
        WorkerModuleFilter currentAppModuleFilter = new WorkerModuleFilter();
        for (Class<? extends AbstractModule> module : allGuiceModules) {
            if (null != currentAppModuleFilter.apply(module)) {
                try {
                    Class[] cArg = new Class[1];
                    cArg[0] = String.class;
                    Constructor<? extends AbstractModule> moduleWithArgs = module.getDeclaredConstructor(cArg);
                    moduleList.add(moduleWithArgs.newInstance(hostName));
                } catch (InstantiationException e) {
                    throw new RuntimeException("Unable to instantiate guice module :" + module, e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Unable to access guice module constructor:" + module, e);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException("Unable to access guice module constructor:" + module, e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException("Unable to access guice module constructor:" + module, e);
                }
            }
        }
        if (!moduleList.isEmpty())
            return moduleList;
        else
            throw new RuntimeException(
                    "Top level app module is not provided. Annotate with WorkerModule & give appropriate name");
    }

    public static void main(String args[]) throws WorkerException {
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(opts, args);
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        String instanceId = cmd.getOptionValue('i');
        String hostName = cmd.getOptionValue('h');
        String nameSpace = cmd.getOptionValue('n');
        new Bootstrap(instanceId, nameSpace, hostName).start();
    }

    private WorkerManager startApp(String appName, Injector injector) throws WorkerException {
        CuratorFramework client = injector.getInstance(CuratorFramework.class);
        WorkerTaskFactory taskFactory = injector.getInstance(WorkerTaskFactory.class);
        TaskList taskList = injector.getInstance(TaskList.class);
        MetricRegistry metricRegistry = injector.getInstance(MetricRegistry.class);
        String listenerPath = buildListenerPath(appName);
        try {
            DistributionManager distributionManager =
                    new DistributionManager(client, taskList, listenerPath, instanceId, metricRegistry);
            WorkerManager workerManager = new WorkerManager(taskFactory, distributionManager);
            workerManager.start();
            return workerManager;
        } catch (Exception ex) {
            log.error("Exception occurred " + ex.fillInStackTrace());
            throw new WorkerException(ex, ErrorCode.WORKER_RUNTIME_ERROR);
        }
    }

    /** adding PathSuffix to appName to segregate listeners for different envs */
    private String buildListenerPath(String appName) {
        String listenerPath = appName;
        if (!Strings.isNullOrEmpty(listenerPathSuffix)) listenerPath += "-" + listenerPathSuffix;
        return listenerPath;
    }

    private void awaitShutdown() {
        lock.lock();
        lock.newCondition().awaitUninterruptibly();
    }

    private static class ShutdownThread extends Thread {
        private final List<WorkerManager> workerManagerList;

        public ShutdownThread(List<WorkerManager> workerManagerList) {
            this.workerManagerList = workerManagerList;
        }

        public void run() {
            for (WorkerManager workerManager : workerManagerList) {
                workerManager.stop();
            }
        }
    }

    private static class WorkerModuleFilter implements Function<Class<? extends AbstractModule>, Class<? extends AbstractModule>> {

        public Class<? extends AbstractModule> apply(Class<? extends AbstractModule> aClass) {
            if (aClass == null) return null;
            WorkerModule annotation = aClass.getAnnotation(WorkerModule.class);
            if (null == annotation) {
                return null;
            }
            return aClass;
        }
    }

    public void setListenerPathSuffix(String listenerPathSuffix) {
        this.listenerPathSuffix = listenerPathSuffix;
    }

    private static Options opts = new Options().addOption(
            OptionBuilder.isRequired(true).hasArgs(1).withLongOpt("instance-id")
                    .withDescription("Instance Id - has to be unique for every instance of jvm (even on single box)")
                    .create('i')).addOption(
            OptionBuilder.isRequired(false).hasArgs(1).withLongOpt("namespace")
                    .withDescription("Namespace - to use to search for workerModule").create('n'))
            .addOption(OptionBuilder.isRequired(false)
                    .hasArgs(1)
                    .withLongOpt("host-name")
                    .withDescription("Host Name")
                    .create('h'));

}
