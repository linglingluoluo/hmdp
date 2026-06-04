package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import jakarta.annotation.Resource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
    private CacheClient cacheClient;
    @Resource
    private ShopServiceImpl shopService;
    @Resource
    private RedisIdWorker  redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    //预热缓存
    @Test
    void testSaveShop() throws InterruptedException {
        Shop shop = shopService.getById(1L);
        cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY + 1L,shop,10L, TimeUnit.SECONDS);
    }
    private ExecutorService es = Executors.newFixedThreadPool(500);
    //测试并发情况下生成id的性能及值的情况
    @Test
    void testIdWorker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);
        long start = System.currentTimeMillis();
        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("id =" + id);
            }
            latch.countDown();
        };
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("time = " + (end - start));
    }

    //将店铺信息存储到redis中
    @Test
    void loadShopData(){
        //1.查询店铺信息
        List<Shop> shopList = shopService.list();
        //2.按照typeId对店铺分组, id一致的放到一个集合里
        Map<Long, List<Shop>> map =
                shopList.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        //3.分批完成写入redis中
        for (Map.Entry<Long, List<Shop>> e : map.entrySet()) {
            //3.1 获取店铺类型id
            Long typeId = e.getKey();
            String key = RedisConstants.SHOP_GEO_KEY + typeId;
            //3.2 获取同类型的店铺集合
            List<Shop> value = e.getValue();
            //店铺的位置集合
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(value.size());
            for (Shop shop : value) {
                //3.3将位置写入到集合里
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(), new Point(shop.getX(), shop.getY())
                ));
            }
            //3.4. 将经纬度写入redis中
            stringRedisTemplate.opsForGeo().add(key, locations);
        }
    }
}
