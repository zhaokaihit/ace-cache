package com.ace.cache.config;

import com.ace.cache.utils.PropertiesLoaderUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.annotation.PostConstruct;
import java.util.concurrent.locks.ReentrantLock;

@Configuration
public class RedisConfig {
    @Autowired
    private Environment env;
    private JedisPool pool;
    private String maxActive;
    private String maxIdle;
    private String maxWait;
    private String host;
    private String password;
    private String timeout;
    private String database;
    private String port;
    private String enable;
    private String sysName;
    private String cluster;

    protected static ReentrantLock lockPool = new ReentrantLock();
    protected static ReentrantLock lockJedis = new ReentrantLock();
    private static JedisPool jedisPool = null;


    @PostConstruct
    public void  init(){
        //PropertiesLoaderUtils prop = new PropertiesLoaderUtils("application.properties");
        PropertiesLoaderUtils prop = new PropertiesLoaderUtils("application.yml");
/*        host = prop.getProperty("redis.host");
        if(StringUtils.isBlank(host)){
            host = env.getProperty("redis.host");
            maxActive = env.getProperty("redis.pool.maxActive");
            maxIdle  = env.getProperty("redis.pool.maxIdle");
            maxWait = env.getProperty("redis.pool.maxWait");
            password = env.getProperty("redis.password");
            timeout = env.getProperty("redis.timeout");
            database = env.getProperty("redis.database");
            port = env.getProperty("redis.port");
            sysName = env.getProperty("redis.sysName");
            enable = env.getProperty("redis.enable");
            cluster = env.getProperty("redis.cluster");
        } else{
            maxActive = prop.getProperty("redis.pool.maxActive");
            maxIdle  = prop.getProperty("redis.pool.maxIdle");
            maxWait = prop.getProperty("redis.pool.maxWait");
            password = prop.getProperty("redis.password");
            timeout = prop.getProperty("redis.timeout");
            database = prop.getProperty("redis.database");
            port = prop.getProperty("redis.port");
            sysName = prop.getProperty("redis.sysName");
            enable = prop.getProperty("redis.enable");
            cluster = prop.getProperty("redis.cluster");
        }*/

        host = prop.getProperty("host");
        if(StringUtils.isBlank(host)){
            host = env.getProperty("redis.host");
            maxActive = env.getProperty("redis.pool.maxActive");
            maxIdle  = env.getProperty("redis.pool.maxIdle");
            maxWait = env.getProperty("redis.pool.maxWait");
            password = env.getProperty("redis.password");
            timeout = env.getProperty("redis.timeout");
            database = env.getProperty("redis.database");
            port = env.getProperty("redis.port");
            sysName = env.getProperty("redis.sysName");
            enable = env.getProperty("redis.enable");
            cluster = env.getProperty("redis.cluster");
        } else{
            maxActive = prop.getProperty("maxActive");
            maxIdle  = prop.getProperty("maxIdle");
            maxWait = prop.getProperty("maxWait");
            password = prop.getProperty("password");
            timeout = prop.getProperty("timeout");
            database = prop.getProperty("database");
            port = prop.getProperty("port");
            sysName = prop.getProperty("sysName");
            enable = prop.getProperty("enable");
            cluster = prop.getProperty("cluster");
        }
    }


    @Bean
    public JedisPoolConfig constructJedisPoolConfig() {

        JedisPoolConfig config = new JedisPoolConfig();
        // 控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
        // 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
        config.setMaxTotal(Integer.parseInt(maxActive));
        // 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
        config.setMaxIdle(Integer.parseInt(maxIdle));
        // 表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
        config.setMaxWaitMillis(Integer.parseInt(maxWait));
        config.setTestOnBorrow(false);

        return config;
    }

    @Bean(name = "pool")
    public JedisPool constructJedisPool() {
        String ip = this.host.split(",")[0];
        int port = Integer.parseInt(this.port.split(",")[0]);
        String password = this.password;
        int timeout = Integer.parseInt(this.timeout);
        int database = Integer.parseInt(this.database);
        boolean cluster = this.cluster.equals("true");
        if(!cluster)
        {
            if (null == pool) {
                if (StringUtils.isBlank(password)) {
                    pool = new JedisPool(constructJedisPoolConfig(), ip, port, timeout);
                } else {
                    pool = new JedisPool(constructJedisPoolConfig(), ip, port, timeout, password, database);
                }
            }
        }
        else
        {
            lockPool.lock();
            try {
                if (jedisPool == null) {
                    jedisPool = new JedisPool(constructJedisPoolConfig(), ip, port, timeout);
                    pool = jedisPool;
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                lockPool.unlock();
            }
        }

        return pool;
    }

    public String getMaxActive() {
        return maxActive;
    }

    public void setMaxActive(String maxActive) {
        this.maxActive = maxActive;
    }

    public String getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(String maxIdle) {
        this.maxIdle = maxIdle;
    }

    public String getMaxWait() {
        return maxWait;
    }

    public void setMaxWait(String maxWait) {
        this.maxWait = maxWait;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTimeout() {
        return timeout;
    }

    public void setTimeout(String timeout) {
        this.timeout = timeout;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getSysName() {
        return sysName;
    }

    public void setSysName(String sysName) {
        this.sysName = sysName;
    }

    public String getEnable() {
        return enable;
    }

    public void setEnable(String enable) {
        this.enable = enable;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }
    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }
}
