package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

//Redis实现全局唯一id
@Component
public class RedisIdWorker {

    private static final long BEGIN__TIMESTAMP = 1779991150;
    public static final int COUNT_BITS = 32;


    private StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    //生成全局唯一id
    public long nextId(String keyPrefix) {
        //1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN__TIMESTAMP;
        //2.生成序列化
        //2.1 获取当前日期,精确到天
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        //将天数拼接在后面,这样每天都有不一样的key,且方便后续统计
        long count = stringRedisTemplate.opsForValue().increment("icr" + keyPrefix + ":" + date);
        //3.拼接并返回
        return timestamp << COUNT_BITS | count;
    }


}
