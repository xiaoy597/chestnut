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

    private static JedisPool jedisPool = null;

    public synchronized static Jedis getJedis(ProjectConfig projectConfig) {
        try {
            if (jedisPool != null) {
                Jedis resource = jedisPool.getResource();
                return resource;
            } else {
                logger.info("Creating jedis pool using password {}...", projectConfig.REDIS_AUTH);

                JedisPoolConfig config = new JedisPoolConfig();
                config.setMaxTotal(projectConfig.REDIS_MAX_TOTAL);
                config.setMaxIdle(projectConfig.REDIS_MAX_IDLE);
                config.setTimeBetweenEvictionRunsMillis(projectConfig.REDIS_MAX_WAIT);
                config.setMinEvictableIdleTimeMillis(projectConfig.REDIS_TIMEOUT);
                config.setTestOnBorrow(projectConfig.REDIS_TEST_ON_BORROW);

                jedisPool = new JedisPool(config,
                        projectConfig.REDIS_HOST_IP,
                        projectConfig.REDIS_HOST_PORT,
                        projectConfig.REDIS_TIMEOUT,
                        projectConfig.REDIS_AUTH);

//                jedisPool = new JedisPool(config,
//                        projectConfig.REDIS_HOST_IP,
//                        projectConfig.REDIS_HOST_PORT,
//                        projectConfig.REDIS_TIMEOUT);

                return jedisPool.getResource();
            }
        } catch (Exception e) {
            logger.error("RedisOperUtil getJedis() failed: ", e);
            return null;
        }
    }

    public static void clearRedisOldKey() {
        try {
            Jedis jedis = getJedis(ProjectConfig.getInstance());

            if (jedis == null)
                return;

            Set<String> oldTagKeys = jedis.keys(ProjectConfig.getInstance().KEY_PREFIX + "*");
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
