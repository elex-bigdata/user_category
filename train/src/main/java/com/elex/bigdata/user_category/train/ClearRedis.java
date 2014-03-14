package com.elex.bigdata.user_category.train;

import com.elex.bigdata.ro.BasicRedisShardedPoolManager;
import redis.clients.jedis.ShardedJedis;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/14/14
 * Time: 11:22 AM
 * To change this template use File | Settings | File Templates.
 */
public class ClearRedis {
  public static void main(String args[]) {
    BasicRedisShardedPoolManager manager = new BasicRedisShardedPoolManager("ClearId", "/redis.site.properties");
    ShardedJedis jedis=null;
    boolean successful=true;
    try {
      jedis = manager.borrowShardedJedis();
      jedis.del(RedisConstans.url_frequent_key);
      jedis.del(RedisConstans.category_frequent_key);
      jedis.del(RedisConstans.url_probability_key);
      jedis.del(RedisConstans.category_probability_key);
    } catch (Exception e) {
      e.printStackTrace();
      successful=false;
    }finally {
      if(successful)
        manager.returnShardedJedis(jedis);
      else
        manager.returnBrokenShardedJedis(jedis);
    }
  }
}
