package com.ace.cache.service.impl;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

import com.ace.cache.config.RedisConfig;
import com.ace.cache.service.IRedisService;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.session.SessionProperties;
import org.springframework.stereotype.Service;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.BinaryClient.LIST_POSITION;

@Service
public class RedisServiceImpl implements IRedisService {
    private static final Logger LOGGER = Logger.getLogger(RedisServiceImpl.class);
    protected static ReentrantLock lockPool = new ReentrantLock();
    protected static ReentrantLock lockJedis = new ReentrantLock();

    @Autowired
    private JedisPool pool;

    private  static RedisConfig redisConfig = new RedisConfig();

    private final static Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
    private static boolean isCluster;

    public RedisServiceImpl(){

        redisConfig.init();
        isCluster=redisConfig.getCluster().equals("true");
        //isCluster = true;
        String[] hosts=redisConfig.getHost().split(",");
        String[] ports=redisConfig.getPort().split(",");
        for (String host:hosts) {
            for (String port:ports)
            {
                jedisClusterNodes.add(new HostAndPort(host, Integer.parseInt(port)));
            }

        }
        /*jedisClusterNodes.add(new HostAndPort("192.168.29.111", 7000));
        jedisClusterNodes.add(new HostAndPort("192.168.29.111", 7001));
        jedisClusterNodes.add(new HostAndPort("192.168.29.111", 7002));
        jedisClusterNodes.add(new HostAndPort("192.168.29.112", 7000));
        jedisClusterNodes.add(new HostAndPort("192.168.29.112", 7001));
        jedisClusterNodes.add(new HostAndPort("192.168.29.112", 7002));
        jedisClusterNodes.add(new HostAndPort("192.168.29.113", 7000));
        jedisClusterNodes.add(new HostAndPort("192.168.29.113", 7001));
        jedisClusterNodes.add(new HostAndPort("192.168.29.113", 7002));
        jedisClusterNodes.add(new HostAndPort("192.168.29.114", 7000));
        jedisClusterNodes.add(new HostAndPort("192.168.29.114", 7001));
        jedisClusterNodes.add(new HostAndPort("192.168.29.114", 7002));*/
    }



    @Override
    public String get(String key) {
        Jedis jedis = null;
        String value = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                value = jc.get(key);
            }
            else
            {
                jedis = pool.getResource();
                value = jedis.get(key);
                returnResource(pool, jedis);
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        } finally {

        }
        return value;
    }

    @Override
    public Set<String> getByPre(String pre) {
        Jedis jedis = null;
        Set<String> value = null;

        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                value = jc.hkeys(pre + "*");
            }
            else
            {
                jedis = pool.getResource();
                value = jedis.keys(pre + "*");
                returnResource(pool, jedis);
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        } finally {

        }
        return value;
    }

    @Override
    public String set(String key, String value) {
        Jedis jedis = null;
        String result = new String();
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                result= jc.set(key, value);
            }
            else
            {
                jedis = pool.getResource();
                returnResource(pool, jedis);
                result= jedis.set(key, value);
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
        return result;
    }

    @Override
    public String set(String key, String value, int expire) {
        Jedis jedis = null;
        String result = new String();
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                int time = jc.ttl(key).intValue() + expire;
                result = jc.set(key, value);
                jc.expire(key, time);
            }
            else
            {
                jedis = pool.getResource();
                int time = jedis.ttl(key).intValue() + expire;
                result = jedis.set(key, value);
                jedis.expire(key, time);
                returnResource(pool, jedis);

            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        } finally {

        }
        return result;
    }

    @Override
    public Long delPre(String key) {
        Jedis jedis = null;
        Long longresult =null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                Set<String> set = jc.hkeys(key + "*");
                int result = set.size();
                Iterator<String> it = set.iterator();
                while (it.hasNext()) {
                    String keyStr = it.next();
                    jc.del(keyStr);
                }
                longresult=new Long(result);
            }
            else
            {
                jedis = pool.getResource();
                Set<String> set = jedis.keys(key + "*");
                int result = set.size();
                Iterator<String> it = set.iterator();
                while (it.hasNext()) {
                    String keyStr = it.next();
                    jedis.del(keyStr);
                }
                returnResource(pool, jedis);

                longresult=new Long(result);
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        } finally {

        }
        return longresult;
    }

    @Override
    public Long del(String... keys) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                //令人遗憾的是jc不能批量删除。。。
                //res = jc.del(keys);
                for(String key:keys) {
                    jc.del(key);
                }
                return res;
            }
            else
            {
                jedis = pool.getResource();
                returnResource(pool, jedis);
                return jedis.del(keys);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            return 0L;
        }  finally {

        }
    }

    @Override
    public Long append(String key, String str) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.append(key, str);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.append(key, str);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
            return 0L;
        } finally {
        }
        return res;
    }

    @Override
    public Boolean exists(String key) {
        Jedis jedis = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                return jc.exists(key);
            }
            else
            {
                jedis = pool.getResource();
                returnResource(pool, jedis);
                return jedis.exists(key);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
            return false;
        } finally {

        }
    }

    @Override
    public Long setnx(String key, String value) {
        Jedis jedis = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                return jc.setnx(key, value);
            }
            else
            {
                jedis = pool.getResource();
                returnResource(pool, jedis);
                return jedis.setnx(key, value);
            }

        } catch (Exception e) {

            LOGGER.error(e.getMessage());
            return 0L;
        } finally {
        }
    }

    @Override
    public String setex(String key, String value, int seconds) {
        Jedis jedis = null;
        String res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.setex(key, seconds, value);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.setex(key, seconds, value);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }

    @Override
    public Long setrange(String key, String str, int offset) {
        Jedis jedis = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                return jc.setrange(key, offset, str);
            }
            else
            {
                jedis = pool.getResource();
                returnResource(pool, jedis);
                return jedis.setrange(key, offset, str);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
            return 0L;
        } finally {
        }
    }

    @Override
    public List<String> mget(String... keys) {
        Jedis jedis = null;
        List<String> values = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                values = jc.mget(keys);
            }
            else
            {
                jedis = pool.getResource();
                values = jedis.mget(keys);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return values;
    }

    @Override
    public String mset(String... keysvalues) {
        Jedis jedis = null;
        String res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.mset(keysvalues);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.mset(keysvalues);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }

    @Override
    public Long msetnx(String... keysvalues) {
        Jedis jedis = null;
        Long res = 0L;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.msetnx(keysvalues);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.msetnx(keysvalues);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }

    @Override
    public String getset(String key, String value) {
        Jedis jedis = null;
        String res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.getSet(key, value);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.getSet(key, value);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }

    @Override
    public String getrange(String key, int startOffset, int endOffset) {
        Jedis jedis = null;
        String res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.getrange(key, startOffset, endOffset);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.getrange(key, startOffset, endOffset);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }

    @Override
    public Long incr(String key) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.incr(key);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.incr(key);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }

    @Override
    public Long incrBy(String key, Long integer) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.incrBy(key, integer);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.incrBy(key, integer);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long decr(String key) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.decr(key);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.decr(key);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long decrBy(String key, Long integer) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.decrBy(key, integer);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.decrBy(key, integer);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long serlen(String key) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.strlen(key);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.strlen(key);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long hset(String key, String field, String value) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.hset(key, field, value);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.hset(key, field, value);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long hsetnx(String key, String field, String value) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.hsetnx(key, field, value);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.hsetnx(key, field, value);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public String hmset(String key, Map<String, String> hash) {
        Jedis jedis = null;
        String res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.hmset(key, hash);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.hmset(key, hash);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public String hget(String key, String field) {
        Jedis jedis = null;
        String res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.hget(key, field);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.hget(key, field);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public List<String> hmget(String key, String... fields) {
        Jedis jedis = null;
        List<String> res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.hmget(key, fields);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.hmget(key, fields);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long hincrby(String key, String field, Long value) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.hincrBy(key, field, value);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.hincrBy(key, field, value);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Boolean hexists(String key, String field) {
        Jedis jedis = null;
        Boolean res = false;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.hexists(key, field);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.hexists(key, field);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long hlen(String key) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.hlen(key);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.hlen(key);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;

    }


    @Override
    public Long hdel(String key, String... fields) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.hdel(key, fields);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.hdel(key, fields);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Set<String> hkeys(String key) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.hkeys(key);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.hkeys(key);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public List<String> hvals(String key) {
        Jedis jedis = null;
        List<String> res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.hvals(key);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.hvals(key);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Map<String, String> hgetall(String key) {
        Jedis jedis = null;
        Map<String, String> res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.hgetAll(key);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.hgetAll(key);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {
            // TODO
        } finally {
        }
        return res;
    }


    @Override
    public Long lpush(String key, String... strs) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.lpush(key, strs);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.lpush(key, strs);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long rpush(String key, String... strs) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.rpush(key, strs);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.rpush(key, strs);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long linsert(String key, LIST_POSITION where, String pivot, String value) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.linsert(key, where, pivot, value);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.linsert(key, where, pivot, value);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public String lset(String key, Long index, String value) {
        Jedis jedis = null;
        String res = null;
        try {
            if(isCluster) {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.lset(key, index, value);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.lset(key, index, value);
                returnResource(pool, jedis);
            }

        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long lrem(String key, long count, String value) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.lrem(key, count, value);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.lrem(key, count, value);
                returnResource(pool, jedis);
            }

        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public String ltrim(String key, long start, long end) {
        Jedis jedis = null;
        String res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.ltrim(key, start, end);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.ltrim(key, start, end);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    synchronized public String lpop(String key) {
        Jedis jedis = null;
        String res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.lpop(key);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.lpop(key);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    synchronized public String rpop(String key) {
        Jedis jedis = null;
        String res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.rpop(key);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.rpop(key);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public String rpoplpush(String srckey, String dstkey) {
        Jedis jedis = null;
        String res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.rpoplpush(srckey, dstkey);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.rpoplpush(srckey, dstkey);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public String lindex(String key, long index) {
        Jedis jedis = null;
        String res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.lindex(key, index);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.lindex(key, index);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long llen(String key) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.llen(key);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.llen(key);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public List<String> lrange(String key, long start, long end) {
        Jedis jedis = null;
        List<String> res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.lrange(key, start, end);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.lrange(key, start, end);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long sadd(String key, String... members) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.sadd(key, members);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.sadd(key, members);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long srem(String key, String... members) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.srem(key, members);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.srem(key, members);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public String spop(String key) {
        Jedis jedis = null;
        String res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.spop(key);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.spop(key);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Set<String> sdiff(String... keys) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.sdiff(keys);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.sdiff(keys);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long sdiffstore(String dstkey, String... keys) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.sdiffstore(dstkey, keys);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.sdiffstore(dstkey, keys);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Set<String> sinter(String... keys) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.sinter(keys);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.sinter(keys);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long sinterstore(String dstkey, String... keys) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.sinterstore(dstkey, keys);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.sinterstore(dstkey, keys);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Set<String> sunion(String... keys) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.sunion(keys);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.sunion(keys);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long sunionstore(String dstkey, String... keys) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.sunionstore(dstkey, keys);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.sunionstore(dstkey, keys);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long smove(String srckey, String dstkey, String member) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.smove(srckey, dstkey, member);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.smove(srckey, dstkey, member);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long scard(String key) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.scard(key);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.scard(key);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Boolean sismember(String key, String member) {
        Jedis jedis = null;
        Boolean res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.sismember(key, member);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.sismember(key, member);
                returnResource(pool, jedis);
            }

        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public String srandmember(String key) {
        Jedis jedis = null;
        String res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.srandmember(key);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.srandmember(key);
                returnResource(pool, jedis);
            }

        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Set<String> smembers(String key) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.smembers(key);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.smembers(key);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }

    @Override
    public Long zadd(String key, double score, String member) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.zadd(key, score, member);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.zadd(key, score, member);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }

    @Override
    public Long zrem(String key, String... members) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.zrem(key, members);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.zrem(key, members);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Double zincrby(String key, double score, String member) {
        Jedis jedis = null;
        Double res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.zincrby(key, score, member);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.zincrby(key, score, member);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long zrank(String key, String member) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.zrank(key, member);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.zrank(key, member);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long zrevrank(String key, String member) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.zrevrank(key, member);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.zrevrank(key, member);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Set<String> zrevrange(String key, long start, long end) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.zrevrange(key, start, end);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.zrevrange(key, start, end);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Set<String> zrangebyscore(String key, String max, String min) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.zrevrangeByScore(key, max, min);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.zrevrangeByScore(key, max, min);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Set<String> zrangeByScore(String key, double max, double min) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.zrevrangeByScore(key, max, min);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.zrevrangeByScore(key, max, min);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long zcount(String key, String min, String max) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.zcount(key, min, max);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.zcount(key, min, max);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long zcard(String key) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.zcard(key);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.zcard(key);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Double zscore(String key, String member) {
        Jedis jedis = null;
        Double res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.zscore(key, member);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.zscore(key, member);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long zremrangeByRank(String key, long start, long end) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.zremrangeByRank(key, start, end);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.zremrangeByRank(key, start, end);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Long zremrangeByScore(String key, double start, double end) {
        Jedis jedis = null;
        Long res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.zremrangeByScore(key, start, end);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.zremrangeByScore(key, start, end);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }


    @Override
    public Set<String> keys(String pattern) {
        Jedis jedis = null;
        Set<String> res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.hkeys(pattern);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.keys(pattern);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }

    @Override
    public String type(String key) {
        Jedis jedis = null;
        String res = null;
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = jc.type(key);
            }
            else
            {
                jedis = pool.getResource();
                res = jedis.type(key);
                returnResource(pool, jedis);
            }
        } catch (Exception e) {

            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }

    /**
     * 返还到连接池
     *
     * @param pool
     * @param jedis
     */
    private static void returnResource(JedisPool pool, Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    @Override
    public Date getExpireDate(String key) {
        Jedis jedis = null;

        Date res = new Date();
        try {
            if(isCluster)
            {
                JedisCluster jc = new JedisCluster(jedisClusterNodes);
                res = new DateTime().plusSeconds(jc.ttl(key).intValue()).toDate();
            }
            else
            {
                jedis = pool.getResource();
                res = new DateTime().plusSeconds(jedis.ttl(key).intValue()).toDate();
                returnResource(pool, jedis);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        } finally {
        }
        return res;
    }
}
