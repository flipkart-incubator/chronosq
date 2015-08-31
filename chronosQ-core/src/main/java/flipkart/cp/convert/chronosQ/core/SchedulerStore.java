package flipkart.cp.convert.chronosQ.core;

import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;

import java.util.List;

public interface SchedulerStore {

    void add(String value, long time, int partitionNum) throws SchedulerException;

    Long update(String value, long oldTime, long newTime, int partitionNum) throws SchedulerException;

    Long remove(String value, long time, int partitionNum) throws SchedulerException;

    List<String> get(long time, int partitionNum) throws SchedulerException;

    List<String> getNextN(long time, int partitionNum, int n) throws SchedulerException;

    void removeBulk(long time, int partitionNum, List<String> values) throws SchedulerException;
}
