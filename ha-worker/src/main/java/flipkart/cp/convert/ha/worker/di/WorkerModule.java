package flipkart.cp.convert.ha.worker.di;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Created by pradeep on 27/02/15.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface WorkerModule {

    String appName();

}
