package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Component
@Slf4j
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    //将任意Java对象序列化为json并存储在string类型的key中，并且可以设置TTL过期时间
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    //将任意Java对象序列化为json并存储在string类型的key中，并且可以设置逻辑过期时间，用于处理缓存击穿问题
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        //设置逻辑过期
        RedisData data = new RedisData();
        data.setData(value);
//        data.setExpireTime(LocalDateTime.now().plus(time, unit.toChronoUnit()));
        data.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(data));
    }

    /**
     * 根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题
     *
     * @param keyPrefix  数据在redis的id前缀
     * @param id         要查询的id
     * @param type       返回的数据类型的class
     * @param dbFallBack 函数式接口,传递查询数据库逻辑,参数类型ID,返回值类型R
     * @param <R>        返回的数据类型shop...
     * @param <ID>       传入id数据类型Integer,Long...
     * @param time       缓存失效时间值
     * @param unit       缓存失效时间单位
     * @return 返回的数据
     */
    public <R, ID> R queryWithCachePenetration(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallBack, Long time, TimeUnit unit) {
        //1.从redis查询缓存
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            //2.1 存在直接返回
            return JSONUtil.toBean(json, type);
        }
        //判断命中的是否是空值, 不是null就是空字符串""
        if (json != null) {
            return null;
        }

        //4.不存在，根据id查询数据库, 怎么去泛型R对应的数据库查 ?
        //函数式编程: 由当前函数调用者传入对应逻辑Function<ID,R> dbFallBack, apply之后就得到了对应返回值
//        R r = getById(id); //单参单返回值用Function
        R r = dbFallBack.apply(id);
        //不存在，返回错误信息
        if (r == null) {
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", Duration.ofMinutes(RedisConstants.CACHE_NULL_TTL));
            //返回错误
            return null;
        }

        //然后再写入redis, 设置缓存失效时间
        this.set(key, r, time, unit);

        //返回
        return r;
    }
    //缓存重建的线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 根据指定的key查询缓存，并反序列化为指定类型，需要利用逻辑过期解决缓存击穿问题
     * @param keyPrefix 数据在redis的前缀
     * @param id         要查询的id
     * @param type       返回的数据类型的class
     * @param dbFallBack 函数式接口,传递查询数据库逻辑,参数类型ID,返回值类型R
     * @param <R>        返回的数据类型shop...
     * @param <ID>       传入id数据类型Integer,Long...
     * @param time       缓存逻辑过期时间值
     * @param unit       缓存逻辑过期时间单位
     * @return 返回的数据
     */
    public <ID,R> R queryWithLogicalExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallBack, Long time, TimeUnit unit) {
        //1.从redis查询商铺缓存
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isBlank(json)) {
            //未命中,直接返回空
            return null;
        }
        //3.命中,需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);

        //4.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //4.1未过期,直接返回店铺信息
            return r;
        }
        //4.2 已过期,需要重建缓存
        //5.缓存重建
        //5.1获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        //5.2判断是否获取互斥锁成功
        if(isLock){
            //5.3获取锁成功,开启独立线程,实现线程重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                //重建缓存
                try {
                    //先查数据库
                    R r1 = dbFallBack.apply(id);
                    //带入逻辑过期时间写入redis,  一般是30min,为了测试设置30s
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e){
                    throw new RuntimeException(e);
                } finally {
                    //释放锁, 不管缓存重建成功还是失败
                    unlock(lockKey);
                }
            });
        }
        //6.返回过期的店铺商品信息
        return r;
    }

    //释放锁
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    //加锁, true代表加锁成功, false表示加锁失败,已经被别的线程加锁了
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", Duration.ofSeconds(10));
        return BooleanUtil.isTrue(flag);
    }
}
