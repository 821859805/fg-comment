package com.fg.shop.service.impl;


import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fg.common.constants.RedisConstants;
import com.fg.common.constants.SystemConstants;
import com.fg.common.model.Result;
import com.fg.shop.mapper.ShopMapper;
import com.fg.shop.service.IShopService;
import com.fg.common.utils.CacheClient;
import com.fg.common.utils.RedisData;
import com.fg.shop.model.po.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryWithPassThrouh ( id );

        //互斥锁解决缓存击穿
        //Shop shop = queryWithMutex ( id );

        HashMap<Integer, Integer> map = new HashMap<>();

        Shop shop = cacheClient.queryWithPassThrouh(RedisConstants.CACHE_SHOP_KEY, id, Shop.class
                , id2 -> getById(id2), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //逻辑过期解决缓存击穿
        /*Shop shop = cacheClient.queryWithLogicalExpire ( RedisConstants.CACHE_SHOP_KEY,id,Shop.class,this::getById,
            RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);*/
        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        return Result.ok(shop);
    }

    /*
     * 逻辑过期解决缓存击穿
     * */
    public Shop queryWithLogicalExpire(Long id) {
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        if (StrUtil.isBlank(shopJson)) {
            //Shop shop = JSONUtil.toBean ( shopJson,Shop.class );
            return null;
        }

        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        if (expireTime.isAfter(LocalDateTime.now())) {
            return shop;
        }
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        if (isLock) {
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    this.saveShop2Redis(id, RedisConstants.CACHE_SHOP_TTL);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }

            });
        }


        return shop;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    /**
     * 缓存穿透代码
     */
    public Shop queryWithPassThrouh(Long id) {
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        if (shopJson != null) {
            return null;
        }
        Shop shop = getById(id);
        if (shop == null) {
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);

            return null;
        }
        shopJson = JSONUtil.toJsonStr(shop);
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, shopJson, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    /*
     *//*
     * 缓存击穿代码
     * *//*
    public Shop queryWithMutex(Long id){
        String shopJson = stringRedisTemplate.opsForValue ().get ( RedisConstants.CACHE_SHOP_KEY + id );
        if (StrUtil.isNotBlank ( shopJson )){
            Shop shop = JSONUtil.toBean ( shopJson,Shop.class );
            return shop;
        }
        if (shopJson!=null){
            return null;
        }
        Shop shop = null;
        String lockKey = RedisConstants.LOCK_SHOP_KEY+id;
        try {
            boolean lock = tryLock ( lockKey );
            if (!lock){
                Thread.sleep ( 50 );
                return queryWithMutex ( id );
            }
            shop = getById ( id );
            if (shop == null){
                stringRedisTemplate.opsForValue ().set ( RedisConstants.CACHE_SHOP_KEY + id,"",RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES );
                return null;
            }
            shopJson = JSONUtil.toJsonStr ( shop );
            stringRedisTemplate.opsForValue ().set ( RedisConstants.CACHE_SHOP_KEY + id,shopJson,RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES );
        }catch (InterruptedException e){
            throw new RuntimeException ( e );
        }finally {
            unlock ( lockKey );
        }
        return shop;
    }*/

    public void saveShop2Redis(Long id, Long expireSeconds) {
        Shop shop = getById(id);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        log.info("{}", redisData);
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }


    @Override
    @Transactional
    public Result update(Shop shop) {
        if (shop.getId() == null) {
            return Result.fail("店铺id不能为空");
        }
        updateById(shop);
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + shop.getId());

        return Result.ok();
    }


    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.MINUTES);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        if (x == null || y == null) {
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        String key = RedisConstants.SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(key, GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs()
                                .includeDistance().limit(end));
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();

        if (list.size() <= from) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            String shopIdStr = result.getContent().getName();
            Distance distance = result.getDistance();
            ids.add(Long.valueOf(shopIdStr));
            distanceMap.put(shopIdStr, distance);
        });
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }
}
