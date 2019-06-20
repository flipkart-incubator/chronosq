package flipkart.cp.convert.chronosQ.core;


import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;

import java.util.List;
import java.util.concurrent.Future;

public interface SchedulerSink {

    Future<?> giveExpiredForProcessing(String value) throws SchedulerException;

    Future<?> giveExpiredListForProcessing(List<String> value) throws SchedulerException;

}
