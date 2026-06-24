package com.hmdp;


import org.json.JSONObject;
import com.hmdp.entity.Shop;
import com.hmdp.entity.User;
import com.hmdp.service.IUserService;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.MQConstants;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import jakarta.annotation.Resource;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedWriter;
import java.io.FileWriter;
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
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    //预热缓存
    @Test
    void testSaveShop() throws InterruptedException {
        Shop shop = shopService.getById(1L);
        cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY + 1L, shop, 10L, TimeUnit.SECONDS);
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
    void loadShopData() {
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


    @Resource
    private RabbitTemplate rabbitTemplate;

    @Test

    public void testSendMessage() {
        rabbitTemplate.convertAndSend(MQConstants.VOUCHER_EXCHANGE_NAME, "direct.seckill", "测试发送消息");
    }


    @Autowired
    private IUserService userService;

    /**
     * 批量导出手机号和token到tokens.txt中, 要提前注释掉UserServiceImpl的login的验证码那行代码
     *         if (redisCode == null || !redisCode.equals(code)) {
     *             //不一致, 报错
     *             return Result.fail("验证码错误");
     *         }
     */
    @Test
    public void function() {
        String loginUrl = "http://localhost:8080/api/user/login"; // 替换为实际的登录URL
        String tokenFilePath = "tokens.txt"; // 存储Token的文件路径

        try {
            HttpClient httpClient = HttpClients.createDefault();

            BufferedWriter writer = new BufferedWriter(new FileWriter(tokenFilePath));

            // 从数据库中获取用户手机号
            List<User> users = userService.list();

            for (User user : users) {
                String phoneNumber = user.getPhone();

                // 构建登录请求
                HttpPost httpPost = new HttpPost(loginUrl);
                //（1.如果作为请求参数传递）
                //List<NameValuePair> params = new ArrayList<>();
                //params.add(new BasicNameValuePair("phone", phoneNumber));                // 如果登录需要提供密码，也可以添加密码参数
                // params.add(new BasicNameValuePair("password", "user_password"));
                //httpPost.setEntity(new UrlEncodedFormEntity(params));                // (2.如果作为请求体传递)构建请求体JSON对象
                JSONObject jsonRequest = new JSONObject();
                jsonRequest.put("phone", phoneNumber);
                StringEntity requestEntity = new StringEntity(
                        jsonRequest.toString(),
                        ContentType.APPLICATION_JSON);
                httpPost.setEntity(requestEntity);

                // 发送登录请求
                HttpResponse response = httpClient.execute(httpPost);

                // 处理登录响应，获取token
                if (response.getStatusLine().getStatusCode() == 200) {
                    HttpEntity entity = response.getEntity();
                    String responseString = EntityUtils.toString(entity);
                    System.out.println(responseString);
                    // 解析响应，获取token，这里假设响应是JSON格式的
                    // 根据实际情况使用合适的JSON库进行解析
                    String token = parseTokenFromJson(responseString);
                    System.out.println("手机号 " + phoneNumber + " 登录成功，Token: " + token);
                    // 将token写入txt文件
                    writer.write(token);
                    writer.newLine();
                } else {
                    System.out.println("手机号 " + phoneNumber + " 登录失败");
                }
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }    // 解析JSON响应获取token的方法，这里只是示例，具体实现需要根据实际响应格式进行解析

    private static String parseTokenFromJson(String json) {
        try {
            // 将JSON字符串转换为JSONObject
            JSONObject jsonObject = new JSONObject(json);
            // 从JSONObject中获取名为"token"的字段的值
            String token = jsonObject.getString("data");
            return token;
        } catch (Exception e) {
            e.printStackTrace();
            return null; // 解析失败，返回null或者抛出异常，具体根据实际需求处理
        }
    }
}
