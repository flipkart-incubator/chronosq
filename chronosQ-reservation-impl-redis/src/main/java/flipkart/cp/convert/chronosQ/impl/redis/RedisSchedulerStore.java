package flipkart.cp.convert.chronosQ.impl.redis;

import flipkart.cp.convert.chronosQ.core.SchedulerStore;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.util.Pool;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
    public void add(String value, long time, int partitionNum) throws SchedulerException {
        Jedis jedis = null;
        String key = "";
        try {
            jedis = _getInstance(partitionNum);
            key = getKey(time, partitionNum);
            jedis.sadd(key, value);
            log.info("Added value " + value + "To " + key);
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + value + "Key" + key + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((jedis != null))
                jedis.close();
        }
    }

    @Override
    public Long update(String value, long oldTime, long newTime, int partitionNum) throws SchedulerException {
        Jedis jedis = null;
        Long result;
        String oldKey = "";
        String newKey = "";
        try {
            jedis = _getInstance(partitionNum);
            oldKey = getKey(oldTime, partitionNum);
            newKey = getKey(newTime, partitionNum);
            result = jedis.smove(oldKey, newKey, value);
            log.info("Updated value " + value + "From " + oldKey + "To " + newKey);
            return result;
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + value + "Key" + oldKey + "Partition " + partitionNum + "-" + ex.getMessage());
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
    public List<String> get(long time, int partitionNum) throws SchedulerException {
        Jedis jedis = null;
        Set<String> resultSet;
        String key = "";
        try {
            jedis = _getInstance(partitionNum);
            key = getKey(time, partitionNum);
            resultSet = jedis.smembers(key);
            log.info("Get For " + key + "-" + resultSet);
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + "Key" + key + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((jedis != null))
                jedis.close();
        }
        return new ArrayList<String>(resultSet);
    }


    @Override
    public List<String> getNextN(long time, int partitionNum, int n) throws SchedulerException {
        Jedis jedis = null;
        List<String> resultSet;
        String key = "";
        try {
            jedis = _getInstance(partitionNum);
            key = getKey(time, partitionNum);
            resultSet = jedis.srandmember(key, n);
            log.info("Get For " + key + "-" + resultSet);
        } catch (Exception ex) {
            log.error("Exception occurred  for -" + "Key" + key + "Partition " + partitionNum + "-" + ex.getMessage());
            throw new SchedulerException(ex, ErrorCode.DATASTORE_READWRITE_ERROR);
        } finally {
            if ((jedis != null))
                jedis.close();
        }
        return new ArrayList<String>(resultSet);
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

    private static String convertNumToString(long time) {
        return String.valueOf(time);
    }

}
