package flipkart.cp.convert.chronosQ.impl.redis;

import flipkart.cp.convert.chronosQ.core.SchedulerStore;
import flipkart.cp.convert.chronosQ.exceptions.ErrorCode;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.util.Pool;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class RedisSchedulerStore implements SchedulerStore {
    private final RedisParitioner redisParitioner;
    private static final String DELIMITER = "-";
    static Logger log = LoggerFactory.getLogger(RedisSchedulerStore.class.getName());

    public RedisSchedulerStore(RedisParitioner redisParitioner) {
        this.redisParitioner = redisParitioner;
    }


    private void _releaseToPool(Jedis jedis, int partitionNum) throws SchedulerException {
        try {
            Pool<Jedis> jedisPool = redisParitioner.getJedis(partitionNum);
            jedisPool.returnResource(jedis);
        } catch (Exception e) {
            throw new SchedulerException(e, ErrorCode.DATASTORE_READWRITE_ERROR);
        }
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
                _releaseToPool(jedis, partitionNum);
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
                _releaseToPool(jedis, partitionNum);
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
                _releaseToPool(jedis, partitionNum);
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
                _releaseToPool(jedis, partitionNum);
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
                _releaseToPool(jedis, partitionNum);
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
                _releaseToPool(jedis, partitionNum);
        }
    }


    private static String getKey(long time, int partitionNum) {
        return convertNumToString(time) + DELIMITER + convertNumToString(partitionNum);
    }

    private static String convertNumToString(long time) {
        return String.valueOf(time);
    }

}
