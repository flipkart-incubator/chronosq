package flipkart.cp.convert.chronosQ.example;


import com.codahale.metrics.MetricRegistry;
import flipkart.cp.convert.chronosQ.client.SchedulerClient;
import flipkart.cp.convert.chronosQ.core.Partitioner;
import flipkart.cp.convert.chronosQ.core.SchedulerEntry;
import flipkart.cp.convert.chronosQ.core.TimeBucket;
import flipkart.cp.convert.chronosQ.core.impl.RandomPartitioner;
import flipkart.cp.convert.chronosQ.core.impl.SecondGroupedTimeBucket;
import flipkart.cp.convert.chronosQ.exceptions.SchedulerException;
import flipkart.cp.convert.chronosQ.impl.redis.RedisParitioner;
import flipkart.cp.convert.chronosQ.impl.redis.RedisSchedulerStore;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.util.Pool;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Example {
    public static void main(String[] args) throws SchedulerException {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(10);
        JedisPool jedisPool = new JedisPool(config, "localhost", 6379);
        SchedulerEntry schedulerEntry = new ExampleSchedulerEntry("BaraniTest", "BaraniTest-ExampleValue");
        long timeInMilliSecs = new Date().getTime();
        Partitioner partitioner = new RandomPartitioner(1);
        List<Pool<Jedis>> jedisPoolList = new ArrayList<Pool<Jedis>>();
        jedisPoolList.add(jedisPool);
        RedisParitioner redisParitioner = new RedisParitioner(jedisPoolList);
        RedisSchedulerStore redisSchedulerStore = new RedisSchedulerStore(redisParitioner, "namespace");
        TimeBucket subMinuteTimeBucket = new SecondGroupedTimeBucket(20);
        MetricRegistry metricRegistry = new MetricRegistry();
        SchedulerClient schedulerClient = new SchedulerClient(redisSchedulerStore, subMinuteTimeBucket, partitioner, metricRegistry);
        schedulerClient.add(schedulerEntry, timeInMilliSecs);
    }

}
