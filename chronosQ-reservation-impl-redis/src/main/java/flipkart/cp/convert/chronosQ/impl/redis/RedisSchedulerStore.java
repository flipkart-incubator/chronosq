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
    public void add(SchedulerEntry schedulerEntry, long time, int partitionNum) throws SchedulerException {
        String key = "";
        try (Jedis jedis = _getInstance(partitionNum)) {
            key = getKey(time, partitionNum);
            jedis.sadd(key, schedulerEntry.getKey());
            jedis.set(getPayloadKey(schedulerEntry.getKey()), schedulerEntry.getPayload());
            log.info("Added value " + schedulerEntry.getKey() + "To " + key);
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + schedulerEntry.getKey() + "Key" + key + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        }
    }

    @Override
    public Long update(SchedulerEntry schedulerEntry, long oldTime, long newTime, int partitionNum) throws SchedulerException {
        Long result;
        String oldKey = "";
        String newKey = "";
        try (Jedis jedis = _getInstance(partitionNum)) {
            oldKey = getKey(oldTime, partitionNum);
            newKey = getKey(newTime, partitionNum);
            result = jedis.smove(oldKey, newKey, schedulerEntry.getKey());
            log.info("Updated value " + schedulerEntry.getKey() + "From " + oldKey + "To " + newKey);
            return result;
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + schedulerEntry.getKey() + "Key" + oldKey + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        }

    }

    @Override
    public Long remove(String value, long time, int partitionNum) throws SchedulerException {
        String key = "";
        try (Jedis jedis = _getInstance(partitionNum)) {
            key = getKey(time, partitionNum);
            Long result = jedis.srem(key, value);
            jedis.del(getPayloadKey(value));
            log.info("Removed value " + value + "From" + key);
            return result;
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + value + "Key" + key + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        }
    }

    @Override
    public List<SchedulerEntry> get(long time, int partitionNum) throws SchedulerException {
        List<SchedulerEntry> schedulerDataList;
        Set<String> resultSet;
        String key = "";
        try (Jedis jedis = _getInstance(partitionNum)) {
            key = getKey(time, partitionNum);
            resultSet = jedis.smembers(key);
            log.info("Get For " + key + "-" + resultSet);
            schedulerDataList = getSchedulerPayloadValues(partitionNum, resultSet);
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + "Key" + key + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        }
        return schedulerDataList;
    }


    @Override
    public List<SchedulerEntry> getNextN(long time, int partitionNum, int n) throws SchedulerException {
        List<String> resultSet;
        List<SchedulerEntry> schedulerDataList;
        String key = "";
        try (Jedis jedis = _getInstance(partitionNum)) {
            key = getKey(time, partitionNum);
            resultSet = jedis.srandmember(key, n);
            log.info("Get For " + key + "-" + resultSet);
            schedulerDataList = getSchedulerPayloadValues(partitionNum, resultSet);
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + "Key" + key + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        }
        return schedulerDataList;
    }

    private List<SchedulerEntry> getSchedulerPayloadValues(int partitionNum, Collection<String> resultSet) throws SchedulerException {
        List<SchedulerEntry> schedulerDataList = new ArrayList<>();
        if (resultSet.isEmpty())
            return schedulerDataList;
        try (Jedis jedis = _getInstance(partitionNum)) {
            Set<String> payloadKeys = resultSet.stream().map(this::getPayloadKey).collect(Collectors.toSet());
            //https://stackoverflow.com/questions/174093/toarraynew-myclass0-or-toarraynew-myclassmylist-size
            String[] keys = payloadKeys.toArray(new String[0]);
            List<String> schedulerValues = jedis.mget(keys);
            Iterator<String> keyIterator = resultSet.iterator();
            Iterator<String> valueIterator = schedulerValues.iterator();
            while (keyIterator.hasNext() && valueIterator.hasNext()) {
                String key = keyIterator.next();
                String value = Optional.ofNullable(valueIterator.next()).orElse(key);
                schedulerDataList.add(new DefaultSchedulerEntry(key, value));
            }
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + "mget payload for Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        }
        return schedulerDataList;
    }

    @Override
    public void removeBulk(long time, int partitionNum, List<String> values) throws SchedulerException {
        String key = "";
        try (Jedis jedis = _getInstance(partitionNum)) {
            Pipeline pipeline = jedis.pipelined();
            key = getKey(time, partitionNum);
            for (String value : values) {
                pipeline.srem(key, value);
                pipeline.del(getPayloadKey(value));
            }
            log.info("Removed values " + values + "From" + key);
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + values + "Key" + key + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
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
