package com.elex.bigdata.user_category.train;

import com.elex.bigdata.ro.BasicRedisShardedPoolManager;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/6/14
 * Time: 5:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestConfig {
  @Test
  public void testRedis(){
    BasicRedisShardedPoolManager manager=new BasicRedisShardedPoolManager("BayesTrainer", "/redis.site.properties");
    manager.borrowShardedJedis();
  }
}
