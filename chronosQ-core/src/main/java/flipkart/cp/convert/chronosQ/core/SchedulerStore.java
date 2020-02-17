package flipkart.cp.convert.chronosQ.core;

import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;

import java.util.List;

public interface SchedulerStore {

    void add(SchedulerData value, long time, int partitionNum) throws SchedulerException;

    Long update(SchedulerData value, long oldTime, long newTime, int partitionNum) throws SchedulerException;

    Long remove(String value, long time, int partitionNum) throws SchedulerException;

    List<SchedulerData> get(long time, int partitionNum) throws SchedulerException;

    List<SchedulerData> getNextN(long time, int partitionNum, int n) throws SchedulerException;

    void removeBulk(long time, int partitionNum, List<String> values) throws SchedulerException;
}
