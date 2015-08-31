package flipkart.cp.convert.chronosQ.core;


import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;

import java.util.List;

public interface SchedulerSink {

    void giveExpiredForProcessing(String value) throws SchedulerException;

    void giveExpiredListForProcessing(List<String> value) throws SchedulerException;

}
