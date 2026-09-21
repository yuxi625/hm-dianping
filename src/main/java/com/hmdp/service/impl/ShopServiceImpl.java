package com.hmdp.service.impl;

import cn.hutool.core.thread.ThreadFactoryBuilder;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONBeanParser;
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
import org.springframework.boot.autoconfigure.cache.CacheProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.*;

import static com.hmdp.utils.RedisConstants.*;

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


    @Resource
    private StringRedisTemplate stringRedisTemplate;

    //private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = new ThreadPoolExecutor(
            10,             //核心线程数
            10,                        //最大线程数，fixed模式核心=最大
            0L,                        //空闲线程存活时间
            TimeUnit.MILLISECONDS,     //空闲线程存活时间单位
            new ArrayBlockingQueue<>(200),       // 等待队列，设置合理上限
            Executors.defaultThreadFactory(),            //线程工厂
            new ThreadPoolExecutor.CallerRunsPolicy()    // 拒绝策略
    );


    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryWithPassThrough(id);


        //互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);


        //逻辑过期解决缓存击穿
        Shop shop = queryWithLogicalExpire(id);

        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        //返回
        return Result.ok(shop);
    }

    //使用互斥锁解决缓存击穿
    public Shop queryWithMutex(Long id) {
        String shopKey = CACHE_SHOP_KEY + id;
        //1.从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3.存在，返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }


        //判断命中是否为空值  (不为null那就是空值)
        if (shopJson != null) {
            //返回错误信息
            return null;
        }


        //4.实现缓存重建
        //4.1.获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2.判断是否获取成功
            if (!isLock) {
                //4.3.失败，则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            //二次检查缓存check
            //1.从redis中查询商铺缓存
            shopJson = stringRedisTemplate.opsForValue().get(shopKey);
            //2.判断是否存在
            if (StrUtil.isNotBlank(shopJson)) {
                //3.存在，返回
                return JSONUtil.toBean(shopJson, Shop.class);
            }
            //判断命中是否为空值  (不为null那就是空值)
            if (shopJson != null) {
                //返回错误信息
                return null;
            }

            //4.4.成功，根据id查询数据库
            shop = getById(id);
            //模拟重建的延时
            Thread.sleep(200);
            //5.查完数据库 判断是否存在
            if (shop == null) {
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(shopKey, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                //不存在，返回错误
                return null;
            }
            //6.存在，返回并更新缓存redis
            stringRedisTemplate.opsForValue().set(shopKey, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //7.释放互斥锁
            unLock(lockKey);
        }

        //8.返回
        return shop;
    }

    //使用缓存空值解决缓存穿透
    public Shop queryWithPassThrough(Long id) {
        String shopKey = CACHE_SHOP_KEY + id;
        //1.从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3.存在，返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }


        //判断命中是否为空值  (不为null那就是空值)
        if (shopJson != null) {
            //返回错误信息
            return null;
        }


        //4.不存在 根据id查询数据库
        Shop shop = getById(id);
        //5.查完数据库 判断是否存在
        if (shop == null) {
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(shopKey, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            //不存在，返回错误
            return null;
        }
        //存在，返回并更新缓存redis

        stringRedisTemplate.opsForValue().set(shopKey, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    //使用逻辑过期解决缓存击穿
    public Shop queryWithLogicalExpire(Long id) {
        String shopKey = CACHE_SHOP_KEY + id;
        //1.从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(shopKey);
        //2.判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            // 3.不存在，直接返回null
            return null;
        }

        //4.命中，需要先把json反序列化为对象

        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断是否过期，
        if(expireTime.isAfter(LocalDateTime.now())) {
            //5.1 未过期，直接返回店铺信息
            return shop;
        }

        //5.2 已过期，需要缓存重建
        //6. 缓存重建
        //6.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        //6.2 判断是否获取锁成功
        if(isLock) {
            //获取锁成功，应该再次检测redis缓存是否过期 如果存在则无需重建
            RedisData redisDataShop = JSONUtil.toBean(stringRedisTemplate.opsForValue().get(shopKey), RedisData.class);
            if(redisDataShop.getExpireTime().isAfter(LocalDateTime.now())) {
                //未过期，直接返回店铺信息
                return JSONUtil.toBean((JSONObject)redisDataShop.getData(), Shop.class);
            }

            //6.3 成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit( () -> {
                try{
                    //重建缓存
                    this.saveShop2Redis(id, 20L);
                }catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unLock(lockKey);
                }

            });

        }

        //6.4 返回过期的商铺信息
        return shop;
    }




    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        //1.查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        //2.封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3.写入redis

        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result update(Shop shop) {

        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }

        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);

        return Result.ok();
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }
}
