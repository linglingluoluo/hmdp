package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;

/**
 * <p>
 *  服务实现类
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
        //1.从redis查询商铺缓存
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        if (StrUtil.isNotBlank(shopJson)) {
            //存在，返回
            //为什么返回shop而不直接返回shopJson?
            //返回 shopJson（String）：Spring 会把它当作普通字符串处理，导致前端收到的是 "{\"id\":1}" 这样的转义字符串
            //返回 shop（Object）：Spring 会调用 JSON 序列化器，输出标准的 JSON 对象
            // 返回 shopJson 的实际响应：
            //{
            //    "code": 200,
            //    "data": "{\"id\":1,\"name\":\"店铺名\",\"address\":\"地址\"}"
            //    // ↑ 这是字符串，前端需要 JSON.parse(data) 两次
            //}
            //
            // 返回 shop 的实际响应：
            //{
            //    "code": 200,
            //    "data": {
            //        "id": 1,
            //        "name": "店铺名",
            //        "address": "地址"
            //    }
            //    // ↑ 这是对象，前端可以直接 data.id 使用
            //}
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }
        //判断命中的是否是空值, 不是null就是空字符串""
        if(shopJson != null){
            return Result.fail("店铺信息不存在!");
        }

        //不存在，根据id查询数据库
        Shop shop=getById(id);

        if(shop==null){
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", Duration.ofMinutes(RedisConstants.CACHE_NULL_TTL));
            //不存在，返回错误信息
            return Result.fail("店铺不存在");
        }

        //然后再写入redis, 设置30分钟的缓存失效时间
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), Duration.ofMinutes(RedisConstants.CACHE_SHOP_TTL));

        //返回
        return Result.ok(shop);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺Id不能为空");
        }
        //1.更新数据库,
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
