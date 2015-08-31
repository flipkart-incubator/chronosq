package flipkart.cp.convert.reservation.scheduler.worker.di;

/**
 * Created with IntelliJ IDEA.
 * User: barani.subramanian
 * Date: 10/06/15
 * Time: 1:08 PM
 * To change this template use File | Settings | File Templates.
 */

import com.flipkart.cfgsvc.client.CConfiguration;
import com.flipkart.cfgsvc.client.CScheduledUpdate;
import com.flipkart.cfgsvc.client.IConfiguration;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import flipkart.cp.convert.reservation.scheduler.worker.config.ConmanHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.Charset.defaultCharset;

/**
 * Created by pradeep on 11/03/14.
 */
public class ConmanModule extends AbstractModule {

    private static final Logger logger = LoggerFactory.getLogger(ConmanModule.class);

    private static final String DEFAULT_VERSION = "HEAD";
    private static final String DEFAULT_HOSTNAME = "localhost";

    private static final String ENV_FILE_PATH = "/etc/default/fk-env";
    private static final String RESERVATION_BASE_BUCKET = "fk-w3-reservation-svc";



    private String appName;
    private String appVersion;
    private String hostName;
    private int refreshInterval;



    private ConmanHelper conmanHelper;

    public ConmanHelper getConmanHelper() {
        return conmanHelper;
    }


    public ConmanModule(String appName, String appVersion) {
        this(appName, appVersion, 0, DEFAULT_HOSTNAME);
    }

    public ConmanModule(String appName, String appVersion, String hostName) {
        this(appName, appVersion, 0, hostName);
    }

    public ConmanModule(String appName, String appVersion, int refreshInterval, String hostName) {
        this.appName = appName;
        this.appVersion = appVersion;
        this.hostName = hostName;
        this.refreshInterval = refreshInterval;
    }

    private static String getHostName() {
        File fkenv = new File(ENV_FILE_PATH);
        if (fkenv.exists() && fkenv.canRead()) {
            try {
                String envName = Files.readFirstLine(fkenv, defaultCharset()).trim().toLowerCase();
                if (null != envName && !envName.equals("")) {
                    return envName;
                }
            } catch (IOException ignored) {
            }
        }

        return DEFAULT_HOSTNAME;
    }

    public ConmanModule(String appName) {
        this(appName, DEFAULT_VERSION);
    }

    @Override
    protected void configure() {

        List<String> serviceBucketList = new ArrayList<String>();
        serviceBucketList.add(RESERVATION_BASE_BUCKET);

        logger.info("Initializing ConMan with buckets: " + serviceBucketList);

        ConmanHelper conmanHelper = new ConmanHelper(serviceBucketList);

        bind(ConmanHelper.class).toInstance(conmanHelper);
        this.conmanHelper=conmanHelper;

        logger.info("Conman initialization done");

    }
}

