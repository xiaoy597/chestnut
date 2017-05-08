package com.evergrande.bigdata.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.util.Iterator;
import java.util.Set;

/**
 * Created by hewin on 2016/2/18.
 */
public class RedisOperUtil {

    private final static Logger logger = LoggerFactory.getLogger(RedisOperUtil.class);

    private static JedisPool jedisPool;
    private static JedisPoolConfig config;

    static {
        try {
            config = new JedisPoolConfig();
            config.setMaxTotal(ProjectConfig.REDIS_MAX_TOTAL);
            config.setMaxIdle(ProjectConfig.REDIS_MAX_IDLE);
            config.setTimeBetweenEvictionRunsMillis(ProjectConfig.REDIS_MAX_WAIT);
            config.setMinEvictableIdleTimeMillis(ProjectConfig.REDIS_TIMEOUT);
            config.setTestOnBorrow(ProjectConfig.REDIS_TEST_ON_BORROW);

            String redisHostIP = System.getenv("REDIS_HOST_IP");
            if (redisHostIP == null)
                throw new RuntimeException("Envrionment REDIS_HOST_IP is not defined.");

            int redisPort = Integer.valueOf(System.getenv("REDIS_HOST_PORT"));

            jedisPool = new JedisPool(config, redisHostIP, redisPort, ProjectConfig.REDIS_TIMEOUT, ProjectConfig.REDIS_AUTH);
        } catch (Exception e) {
            logger.error("RedisOperUtil init failed: ", e);
        }
    }

    public synchronized static Jedis getJedis() {
        try {
            if (jedisPool != null) {
                Jedis resource = jedisPool.getResource();
                return resource;
            } else {
                logger.info("RedisOperUtil getJedis() jedisPool is null!");
                jedisPool = new JedisPool(config, ProjectConfig.REDIS_HOST_IP, ProjectConfig.REDIS_HOST_PORT, ProjectConfig.REDIS_TIMEOUT, ProjectConfig.REDIS_AUTH);
                return jedisPool.getResource();
            }
        } catch (Exception e) {
            logger.error("RedisOperUtil getJedis() failed: ", e);
            return null;
        }
    }

    public static void clearRedisOldKey(String keyPrefix) {
        try {
            Jedis jedis = getJedis();

            if (jedis == null)
                return;

            Set<String> oldTagKeys = jedis.keys(keyPrefix + "*");
            Iterator<String> it = oldTagKeys.iterator();
            while (it.hasNext()) {
                String key = it.next();
                jedis.del(key);
            }
            returnResource(jedis);
        } catch (Exception e) {
            logger.error("Exception {} caught when clearing Redis data. \n{}", e.getMessage(), e);
        }
    }

    public static void returnResource(final Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }
}
