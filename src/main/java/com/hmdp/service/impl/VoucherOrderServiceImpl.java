package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */

@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private IVoucherOrderService proxy;


    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while(true) {
                try {
                    //1.获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //1.获取用户
        Long userId = voucherOrder.getUserId();
        //2.创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //3.获取锁
        boolean isLock = lock.tryLock();
        //4.判断是否获取锁成功
        if (!isLock) {
            //获取锁失败，返回错误或重试
            log.error("不允许重复下单");
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

        //5.1查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        //5.2判断是否存在
        if (count > 0) {
            //用户已经购买过了
            log.error("用户已经购买过一次!");
            return;
        }


        //6.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();
        /*
        update
        set stock = stock - 1
        where id = ? ans stock > 0
         */

        if (!success) {
            //扣减失败
            log.error("库存不足!");
            return;
        }

        //7.4 保存订单
        save(voucherOrder);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {

        //获取用户
        Long userId = UserHolder.getUser().getId();

        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString()
        );

        //判断结果是否为0
        int r = result.intValue();
        if(r!=0) {
            //2.1不为0 没有购买资格
            return Result.fail(r == 1 ? "库存不足":"不能重复下单");
        }
        //2.2 为0 有购买资格，把下单信息保存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        //2.3订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //2.4用户id
        voucherOrder.setUserId(userId);
        //2.5代金券id
        voucherOrder.setVoucherId(voucherId);
        //2.6放入阻塞队列
        orderTasks.add(voucherOrder);

        //3.获取代理对象（事务）
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        //4.返回订单id
        return Result.ok(orderId);
    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//
//        //1.查询优惠券信息
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//
//        //2.判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀还未开始");
//        }
//        //3.判断秒杀是否结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已经结束");
//        }
//
//        //4.判断库存是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足");
//        }
//
//
//        Long userId = UserHolder.getUser().getId();
//        //创建锁对象
//        // SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//
//
//        //获取锁
//        boolean isLock = lock.tryLock();
//        //判断是否获取锁成功
//        if (!isLock) {
//            //获取锁失败，返回错误或重试
//            return Result.fail("一个人只允许下一单");
//        }
//
//        try {
//            //获取代理对象（事务）
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            //释放锁
//            lock.unlock();
//        }
//    }
//
//
//    @Transactional
//    public Result createVoucherOrder(Long voucherId) {
//
//        //5.一人一单
//        Long userId = UserHolder.getUser().getId();
//
//        //5.1查询订单
//        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//        //5.2判断是否存在
//        if (count > 0) {
//            return Result.fail("用户已经购买过了!");
//        }
//
//
//        //6.扣减库存
//        boolean success = seckillVoucherService.update()
//                .setSql("stock = stock - 1")
//                .eq("voucher_id", voucherId).gt("stock", 0)
//                .update();
//        /*
//        update
//        set stock = stock - 1
//        where id = ? ans stock > 0
//         */
//
//        if (!success) {
//            //扣减失败
//            return Result.fail("库存不足");
//        }
//
//        //7.创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        //7.1订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        //7.2用户id
//        voucherOrder.setUserId(userId);
//        //7.3代金券id
//        voucherOrder.setVoucherId(voucherId);
//        //7.4 保存订单
//        save(voucherOrder);
//
//        //8.返回订单id
//        return Result.ok(orderId);
//    }




}
