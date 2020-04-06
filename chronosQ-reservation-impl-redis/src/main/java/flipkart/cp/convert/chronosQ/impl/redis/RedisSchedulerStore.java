package flipkart.cp.convert.chronosQ.impl.redis;

import flipkart.cp.convert.chronosQ.core.DefaultSchedulerEntry;
import flipkart.cp.convert.chronosQ.core.SchedulerEntry;
import flipkart.cp.convert.chronosQ.core.SchedulerStore;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.util.Pool;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class RedisSchedulerStore implements SchedulerStore {
    private final RedisParitioner redisParitioner;
    private static final String DELIMITER = "-";
    private final String keyPrefix;

    public RedisSchedulerStore(RedisParitioner redisParitioner) {
        this(redisParitioner, "");
    }

    public RedisSchedulerStore(RedisParitioner redisParitioner, String keyPrefix) {
        this.redisParitioner = redisParitioner;
        this.keyPrefix = keyPrefix;
    }

    private Jedis _getInstance(int partitionNum) throws SchedulerException {
        try {
            Pool<Jedis> jedisPool = redisParitioner.getJedis(partitionNum);
            return jedisPool.getResource();
        } catch (Exception e) {
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        }
    }

    @Override
    public void add(SchedulerEntry schedulerData, long time, int partitionNum) throws SchedulerException {
        Jedis jedis = null;
        String key = "";
        try {
            jedis = _getInstance(partitionNum);
            key = getKey(time, partitionNum);
            jedis.sadd(key, schedulerData.getKey());
            jedis.set(getPayloadKey(schedulerData.getKey()), schedulerData.getPayload());
            log.info("Added value " + schedulerData.getKey() + "To " + key);
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + schedulerData.getKey() + "Key" + key + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((jedis != null))
                jedis.close();
        }
    }

    @Override
    public Long update(SchedulerEntry schedulerData, long oldTime, long newTime, int partitionNum) throws SchedulerException {
        Jedis jedis = null;
        Long result;
        String oldKey = "";
        String newKey = "";
        try {
            jedis = _getInstance(partitionNum);
            oldKey = getKey(oldTime, partitionNum);
            newKey = getKey(newTime, partitionNum);
            result = jedis.smove(oldKey, newKey, schedulerData.getKey());
            log.info("Updated value " + schedulerData.getKey() + "From " + oldKey + "To " + newKey);
            return result;
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + schedulerData.getKey() + "Key" + oldKey + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((jedis != null))
                jedis.close();
        }

    }

    @Override
    public Long remove(String value, long time, int partitionNum) throws SchedulerException {
        Jedis jedis = null;
        String key = "";

        try {
            jedis = _getInstance(partitionNum);
            key = getKey(time, partitionNum);
            Long result = jedis.srem(key, value);
            jedis.del(getPayloadKey(value));
            log.info("Removed value " + value + "From" + key);
            return result;
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + value + "Key" + key + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((jedis != null))
                jedis.close();
        }
    }

    @Override
    public List<SchedulerEntry> get(long time, int partitionNum) throws SchedulerException {
        Jedis jedis = null;
        List<SchedulerEntry> schedulerDataList;
        Set<String> resultSet;
        String key = "";
        try {
            jedis = _getInstance(partitionNum);
            key = getKey(time, partitionNum);
            resultSet = jedis.smembers(key);
            log.info("Get For " + key + "-" + resultSet);
            Set<String> dataKeySet = resultSet.stream().map(this::getPayloadKey).collect(Collectors.toSet());
            schedulerDataList = getSchedulerPayloadValues(partitionNum, dataKeySet);
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + "Key" + key + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((jedis != null))
                jedis.close();
        }
        return schedulerDataList;
    }


    @Override
    public List<SchedulerEntry> getNextN(long time, int partitionNum, int n) throws SchedulerException {
        Jedis jedis = null;
        List<String> resultSet;
        List<SchedulerEntry> schedulerDataList;
        String key = "";
        try {
            jedis = _getInstance(partitionNum);
            key = getKey(time, partitionNum);
            resultSet = jedis.srandmember(key, n);
            log.info("Get For " + key + "-" + resultSet);
            Set<String> dataKeySet = resultSet.stream().map(this::getPayloadKey).collect(Collectors.toSet());
            schedulerDataList = getSchedulerPayloadValues(partitionNum, dataKeySet);
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + "Key" + key + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((jedis != null))
                jedis.close();
        }
        return schedulerDataList;
    }

    private List<SchedulerEntry> getSchedulerPayloadValues(int partitionNum, Collection<String> resultSet) throws SchedulerException {
        List<SchedulerEntry> schedulerDataList = new ArrayList<>();
        Jedis jedis = null;
        if (resultSet.isEmpty())
            return schedulerDataList;
        try {
            jedis = _getInstance(partitionNum);
            //https://stackoverflow.com/questions/174093/toarraynew-myclass0-or-toarraynew-myclassmylist-size
            String[] keys = resultSet.toArray(new String[0]);
            List<String> schedulerValues = jedis.mget(keys);
            Iterator<String> keyIterator = resultSet.iterator();
            Iterator<String> valueIterator = schedulerValues.iterator();
            while (keyIterator.hasNext() && valueIterator.hasNext()) {
                String key = keyIterator.next();
                String value = valueIterator.next();
                value = "nil".equalsIgnoreCase(value) ? key : value;
                schedulerDataList.add(new DefaultSchedulerEntry(key, value));
            }
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + "mget payload for Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((jedis != null))
                jedis.close();
        }
        return schedulerDataList;
    }

    @Override
    public void removeBulk(long time, int partitionNum, List<String> values) throws SchedulerException {
        Jedis jedis = null;
        String key = "";
        try {
            jedis = _getInstance(partitionNum);
            Pipeline pipeline = jedis.pipelined();
            key = getKey(time, partitionNum);
            for (String value : values)
                pipeline.srem(key, value);
            log.info("Removed values " + values + "From" + key);
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + values + "Key" + key + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((jedis != null))
                jedis.close();
        }
    }

    private String getKey(long time, int partitionNum) {
        String prefix = keyPrefix != null && !keyPrefix.equals("") ? keyPrefix + DELIMITER : "";
        return prefix + convertNumToString(time) + DELIMITER + convertNumToString(partitionNum);
    }

    private String getPayloadKey(String rawKey) {
        String prefix = keyPrefix != null && !keyPrefix.equals("") ? keyPrefix + DELIMITER : "";
        return prefix + rawKey;
    }

    private static String convertNumToString(long time) {
        return String.valueOf(time);
    }

}
