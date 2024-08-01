package com.fg.common.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.fg.common.constants.RedisConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;


    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue ().set ( key, JSONUtil.toJsonStr ( value ),time,unit );

    }

    /*
    * 逻辑过期
    * */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData ();
        redisData.setData ( value );
        redisData.setExpireTime ( LocalDateTime.now ().plusSeconds ( unit.toSeconds ( time ) ) );
        stringRedisTemplate.opsForValue ().set ( key, JSONUtil.toJsonStr ( redisData ));
    }

    public <R,ID> R queryWithPassThrouh(String keyPrefix, ID id, Class<R> type,
                                        Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String json = stringRedisTemplate.opsForValue ().get ( keyPrefix+ id );
        log.info ( "查询到的值{}",json );
        if (StrUtil.isNotBlank ( json )){
            return JSONUtil.toBean ( json,type );
        }
        if (json!=null){
            return null;
        }
        R r = dbFallback.apply ( id );
        if (r == null){
            stringRedisTemplate.opsForValue ().set ( keyPrefix + id,"", RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES );

            return null;
        }
        this.set ( keyPrefix+ id,r,time,unit );
        return r;
    }

    /*
     * 逻辑过期解决缓存击穿
     * */
    public <R,ID> R queryWithLogicalExpire(String keyPrefix,ID id,Class<R> type,
                                           Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String json = stringRedisTemplate.opsForValue ().get ( keyPrefix+ id );
        if (StrUtil.isBlank ( json )){
            //Shop shop = JSONUtil.toBean ( shopJson,Shop.class );
            return null;
        }

        RedisData redisData = JSONUtil.toBean ( json, RedisData.class );
        R r = JSONUtil.toBean ( (JSONObject) redisData.getData (), type);
        LocalDateTime expireTime = redisData.getExpireTime ();

        if (expireTime.isAfter ( LocalDateTime.now () )){
            return r;
        }
        String lockKey = RedisConstants.LOCK_SHOP_KEY+id;
        boolean isLock = tryLock ( lockKey );
        if (isLock){
            CACHE_REBUILD_EXECUTOR.submit ( ()->{
                try {
                    R r1 = dbFallback.apply ( id );
                    this.setWithLogicalExpire ( keyPrefix+id,r1,time,unit );
                }catch (Exception e){
                    throw new RuntimeException ( e );
                }finally {
                    unlock ( lockKey );
                }

            } );
        }


        return r;
    }
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool (10);

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue ().setIfAbsent ( key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.MINUTES );
        return BooleanUtil.isTrue ( flag );
    }

    private void unlock(String key){
        stringRedisTemplate.delete ( key );
    }
}
