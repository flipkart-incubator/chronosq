package flipkart.cp.convert.chronosQ.core;

import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;

import java.util.List;

public interface SchedulerStore {

    void add(SchedulerEntry value, long time, int partitionNum) throws SchedulerException;

    Long update(SchedulerEntry value, long oldTime, long newTime, int partitionNum) throws SchedulerException;

    Long remove(String value, long time, int partitionNum) throws SchedulerException;

    List<SchedulerEntry> get(long time, int partitionNum) throws SchedulerException;

    List<SchedulerEntry> getNextN(long time, int partitionNum, int n) throws SchedulerException;

    void removeBulk(long time, int partitionNum, List<String> values) throws SchedulerException;
}
