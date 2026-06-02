package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
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
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Autowired
    private ISeckillVoucherService seckillVoucherService;
    @Autowired
    private RedisIdWorker redisIdWorker;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 2024);
    //异步下单所需新线程的线程池, 单个线程就行
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    //主线程的代理对象
    private IVoucherOrderService proxy;

    @PostConstruct //使当前类初始化后执行
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    //执行异步下单
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    //2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }

    //创建订单,其实可以不用加锁, 因为lua脚本已经做过判断
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //1.获取用户,由于是新的线程,所以不能从ThreadLocal获取userId
        Long userId = voucherOrder.getUserId();
        //2.创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock(); //无参代表失败不等待
        //3.判断是否获取锁成功
        if (!isLock) {
            //获取锁失败, 返回错误或重试
            log.info("不允许重新下单");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            //释放锁
            lock.unlock();
        }
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //5.一人一单
        Long userId = voucherOrder.getUserId();

        //查询订单
        long count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            //用户已经购买过优惠券
            log.error("用户已经该买过一次!");
            return;
        }

        //6.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0) //CAS控制stock
                .update();
        if (!success) {
            log.error("库存不足");
            return;
        }
        //5.6实际可以省略, 因为前面redis中lua脚本已经判断能否创建订单了
        //7.创建订单
        save(voucherOrder);
    }

    //优惠券秒杀
    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户id
        Long userId = UserHolder.getUser().getId();
        //1.执行lua脚本, 尝试判读用户有无购买资格,库存是否充足
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.EMPTY_LIST,
                voucherId.toString(), userId.toString()
        );
        //2.判断结果是否为0
        int res = result.intValue();
        if (res != 0) {
            //2.1 不为0，代表没有购买资格
            return Result.fail(res == 1 ? "库存不足" : "不能重复下单");
        }
        //2.2 为0,有购买资格,把下单信息保存到阻塞队列中
        //2.2.1创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        //2.2.2放入阻塞队列
        orderTasks.add(voucherOrder);

        //3.获取代理对象(事务), 原来在子线程的代码移入主线程中, 因为子线程获取代理对象也是靠ThreadLocal
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //4.返回订单id
        return Result.ok(orderId);
    }
    /*//优惠券秒杀
    @Override
    public Result seckillVoucher(Long voucherId) {
        //1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2.判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            //尚未开始
            return Result.fail("秒杀尚未开始!");
        }
        //3.判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            //已经结束
            return Result.fail("秒杀已经结束!");
        }
        //4.判断库存是否充足
        if (voucher.getStock() < 1) {
            return Result.fail("库存不足");
        }
        Long userId = UserHolder.getUser().getId();
        //创建锁对象
//        SimpleRedisLock lock = new SimpleRedisLock("order" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock(); //无参代表失败不等待
        //判断是否获取锁成功
        if(!isLock){
            //获取锁失败, 返回错误或重试
            return Result.fail("不允许重复下单");
        }
        try {
            //获取代理对象(事务)
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }  finally {
            //释放锁
            lock.unlock();
        }

        //用值判断是否是同一用户, 因为不同线程的userId是不同对象
        //userId.toString()还是会创建一个新对象,intern()是字符串常量池中相等的值
//        synchronized (userId.toString().intern()) {
//            //获取代理对象(事务)
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }
    }*/
    /*
    //一人一单及超卖的解决
    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        //5.一人一单
        Long userId = UserHolder.getUser().getId();

        //查询订单
        long count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            //用户已经购买过优惠券
            return Result.fail("用户已经该买过一次!");
        }

        //6.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock", 0) //CAS控制stock
                .update();
        if (!success) {
            return Result.fail("库存不足");
        }
        //7.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //7.1订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //7.2 用户id
        voucherOrder.setUserId(userId);
        //7.3代金券id
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);
        //8.返回订单id
        return Result.ok(orderId);
    }
*/
}
