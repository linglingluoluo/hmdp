package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {
        //缓存穿透cache penetration
//        Shop shop = queryWithCachePenetration(id);

        //互斥锁解决缓存击穿cache invalid
//        Shop shop = queryWithMutex(id);

        //逻辑过期解决缓存击穿cache invalid
        Shop shop = queryWithLogicalExpire(id);
        if (shop == null) {
            return Result.fail("店铺不存在! ");
        }
        //返回
        return Result.ok(shop);
    }

    /**
     * 利用互斥锁解决查询店铺信息的缓存击穿(cache invalid)
     * @param id
     * @return
     */
    public Shop queryWithMutex(Long id) {
        //1.从redis查询商铺缓存
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断命中的是否是空值, 不是null就是空字符串""
        if (shopJson != null) {
            return null;
        }

        //4.实现缓存重建
        //4.1获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY  + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2判断是否获取成功

            if(!isLock){
                //4.3失败,则休眠重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //4.4获取锁成功,根据id查询数据库
            shop = getById(id);
            //模拟重建的延时
//            Thread.sleep(200);
            //5.不存在,返回错误
            if (shop == null) {
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", Duration.ofMinutes(RedisConstants.CACHE_NULL_TTL));
                //不存在，返回错误信息
                return null;
            }

            //6.存在, 写入redis, 设置30分钟的缓存失效时间
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), Duration.ofMinutes(RedisConstants.CACHE_SHOP_TTL));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //7.释放互斥锁
            unlock(lockKey);
        }

        //8.返回
        return shop;
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
    /**
     * 通过将缓存空对象来解决缓存穿透cache penetration
     *
     * @param id
     * @return
     */
    public Shop queryWithCachePenetration(Long id) {
        //1.从redis查询商铺缓存
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //判断命中的是否是空值, 不是null就是空字符串""
        if (shopJson != null) {
            return null;
        }

        //不存在，根据id查询数据库
        Shop shop = getById(id);

        if (shop == null) {
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", Duration.ofMinutes(RedisConstants.CACHE_NULL_TTL));
            //不存在，返回错误信息
            return null;
        }

        //然后再写入redis, 设置30分钟的缓存失效时间
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), Duration.ofMinutes(RedisConstants.CACHE_SHOP_TTL));

        //返回
        return shop;
    }
    //模拟逻辑过期中, 存入商铺热点key信息
    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        //1. 获取店铺信息
        Shop shop = getById(id);
        //模拟延迟,实际不用加
        Thread.sleep(200);
        //2. 封装数据
        RedisData data = new RedisData();
        data.setData(shop);
        data.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.存入redis中
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(data));
    }
    //缓存重建的线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    /**
     * 利用 逻辑过期解决缓存穿透问题
     * @param id
     * @return
     */
    public Shop queryWithLogicalExpire(Long id) {
        //1.从redis查询商铺缓存
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            //未命中,直接返回空
            return null;
        }
        //3.命中,需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);

        //4.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //4.1未过期,直接返回店铺信息
            return shop;
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
                    this.saveShop2Redis(id, 20L); //一般是30min,为了测试设置30s
                } catch (Exception e){
                    throw new RuntimeException(e);
                } finally {
                    //释放锁, 不管缓存重建成功还是失败
                    unlock(lockKey);
                }
            });
        }
        //6.返回过期的店铺商品信息
        return shop;
    }
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺Id不能为空");
        }
        //1.更新数据库,
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
