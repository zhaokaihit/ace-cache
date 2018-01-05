package com.ace.cache.entity;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

/**
 * 解决问题：
 *
 * @author Ace
 * @version 1.0
 * @date 2017年5月5日
 * @since 1.7
 */
public class ClusterCacheBean {
    private String key = "";
    private String desc = "";
    @JsonFormat(timezone = "GMT+8", pattern = "yyyy-MM-dd HH:mm:ss")
    private Date expireTime;

    public ClusterCacheBean(String key, String desc, Date expireTime) {
        this.key = key;
        this.desc = desc;
        this.expireTime = expireTime;
    }

    public ClusterCacheBean(String key, Date expireTime) {
        this.key = key;
        this.expireTime = expireTime;
    }

    public ClusterCacheBean() {

    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public Date getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(Date expireTime) {
        this.expireTime = expireTime;
    }

}
