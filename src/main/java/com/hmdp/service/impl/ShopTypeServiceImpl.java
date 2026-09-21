package com.hmdp.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result getlist() {


        //1.从缓存中查询
        String shopTypeJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOPTYPE_KEY);

        //2.判断是否存在
        if (StrUtil.isNotBlank(shopTypeJson)) {
            //存在，直接返回
            List<ShopType> shopTypeList = JSONUtil.toList(shopTypeJson, ShopType.class);
            return Result.ok(shopTypeList);
        }

        //3.不存在，查询数据库
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();
        //判断是否存在
        if (CollUtil.isEmpty(shopTypeList)) {
            //不存在，返回错误
            return Result.fail("店铺类型不存在");
        }
        //存在，返回并写入缓存
        stringRedisTemplate.opsForValue().set(
                RedisConstants.CACHE_SHOPTYPE_KEY,
                JSONUtil.toJsonStr(shopTypeList)
        );
        return Result.ok(shopTypeList);
    }
}
