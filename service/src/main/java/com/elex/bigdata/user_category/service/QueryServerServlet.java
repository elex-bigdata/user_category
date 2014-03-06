package com.elex.bigdata.user_category.service;

import com.caucho.hessian.server.HessianServlet;
import com.elex.bigdata.ro.BasicRedisShardedPoolManager;
import org.codehaus.jackson.map.ObjectMapper;
import redis.clients.jedis.ShardedJedis;

import java.util.HashMap;
import java.util.Map;
import static com.elex.bigdata.user_category.service.RedisConstans.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/4/14
 * Time: 6:31 PM
 * To change this template use File | Settings | File Templates.
 */
public class QueryServerServlet extends HessianServlet implements Submit{
  private BasicRedisShardedPoolManager redisShardedPoolManager=null;
  private Map<String,String> url_category=new HashMap<String, String>();
  public QueryServerServlet(){
     redisShardedPoolManager=new BasicRedisShardedPoolManager("user_category_server","redis.site.properties");
     boolean successful=true;
     ShardedJedis shardedJedis=null;
     try{
       shardedJedis=redisShardedPoolManager.borrowShardedJedis();
       url_category=shardedJedis.hgetAll(url_category_key);
     }catch (Exception e){
       successful=false;
       e.printStackTrace();
     }finally {
       if(successful)
         redisShardedPoolManager.returnShardedJedis(shardedJedis);
       else
         redisShardedPoolManager.returnBrokenShardedJedis(shardedJedis);
     }
  }
  @Override
  public void submit(String uid,Map<String, Integer> user) {
    ShardedJedis shardedJedis=null;
    boolean successful=true;
    try{
      shardedJedis=redisShardedPoolManager.borrowShardedJedis();
      Map<String,Double> user_category=inference(shardedJedis,user);
      ObjectMapper mapper=new ObjectMapper();
      String categoryStr=mapper.writeValueAsString(user_category);
      shardedJedis.hset("user_categories",uid,categoryStr);
    }catch(Exception e){
      successful=false;
      e.printStackTrace();
    }finally {
      if(successful)
        redisShardedPoolManager.returnShardedJedis(shardedJedis);
      else
        redisShardedPoolManager.returnBrokenShardedJedis(shardedJedis);
    }
  }

  @Override
  public void submitBatch(Map<String, Map<String, Integer>> users) {
    ShardedJedis shardedJedis=null;
    boolean successful=true;
    try{
      shardedJedis=redisShardedPoolManager.borrowShardedJedis();
      Map<String,Map<String,Double>> user_categories=inferenceBatch(shardedJedis,users);
      ObjectMapper mapper=new ObjectMapper();
      for(Map.Entry<String,Map<String,Double>> entry: user_categories.entrySet()){
        Map<String,Double> user=entry.getValue();
        String uid=entry.getKey();
        String categoryStr=mapper.writeValueAsString(user);
        shardedJedis.hset("user_categories",uid,categoryStr);
      }
    }catch(Exception e){
      successful=false;
      e.printStackTrace();
    }finally {
      if(successful)
        redisShardedPoolManager.returnShardedJedis(shardedJedis);
      else
        redisShardedPoolManager.returnBrokenShardedJedis(shardedJedis);
    }
  }

  protected Map<String,Double>  inference(ShardedJedis shardedJedis,Map<String, Integer> user) {
    double categories_weight = 0;
    Map<String, Double> user_category = new HashMap<String, Double>();
    for (String url : user.keySet()) {
      if (this.url_category.containsKey(url)) {
        String category = this.url_category.get(url);
        double category_probability = Double.valueOf(shardedJedis.hget(category_probability_key, category));
        double url_probability = Double.valueOf(shardedJedis.hget(url_probability_key, url));
        int url_frequent = user.get(url);
        double category_weight = category_probability * url_frequent * url_probability;
        System.out.println(url);
        System.out.println(url_frequent);
        System.out.println(url_probability);
        System.out.println(category);
        System.out.println(category_probability);

        if (user_category.containsKey(category)) {
          user_category.put(category, user_category.get(category) + category_weight);
        } else {
          user_category.put(category, category_weight);
        }
        categories_weight += category_weight;
      }
    }

    for (String category : user_category.keySet()) {
      user_category.put(category, user_category.get(category) / categories_weight);
    }

    return user_category;
  }

  protected Map<String,Map<String,Double>> inferenceBatch(ShardedJedis shardedJedis,Map<String,Map<String,Integer>> users) {
    Map<String,Map<String,Double>> user_categories=new HashMap<String, Map<String, Double>>();
    for(Map.Entry<String,Map<String,Integer>> entry: users.entrySet()){
      String uid=entry.getKey();
      Map<String,Integer> user=entry.getValue();
      Map<String,Double> user_category=inference(shardedJedis,user);
      user_categories.put(uid,user_category);
    }
    return user_categories;
  }


}
