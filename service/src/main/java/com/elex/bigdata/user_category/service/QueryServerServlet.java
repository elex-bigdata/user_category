package com.elex.bigdata.user_category.service;

import com.caucho.hessian.server.HessianServlet;
import com.elex.bigdata.conf.Config;
import com.elex.bigdata.hashing.BDMD5;
import com.elex.bigdata.ro.BasicRedisShardedPoolManager;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import redis.clients.jedis.ShardedJedis;

import java.util.HashMap;
import java.util.Iterator;
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
  private Logger logger=Logger.getLogger(QueryServerServlet.class);
  public QueryServerServlet(){
     redisShardedPoolManager=new BasicRedisShardedPoolManager("user_category_server","/redis.site.properties");
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

  //query Interface for single user: uid ,map<url,frequent>
  @Override
  public void submit(String uid,Map<String, Integer> user) {
    ShardedJedis shardedJedis=null;
    boolean successful=true;
    try{
      shardedJedis=redisShardedPoolManager.borrowShardedJedis();
      Map<String,Double> user_category=inference(shardedJedis,user);

      Map<String,Integer> spUser=getSimplifiedCategories(user_category);
      StringBuilder resultBuilder=new StringBuilder();
      for(Map.Entry<String,Integer> entry1: spUser.entrySet()){
        resultBuilder.append(entry1.getKey());
        // 使用16进制以便只用两位记录比例（防止出现z100的情况)
        resultBuilder.append(entry1.getValue()/16);
        resultBuilder.append(entry1.getValue()%16);
      }
      String categoryStr=resultBuilder.toString();
      String uidMd5= BDMD5.getInstance().toMD5(uid);
      shardedJedis.set(uidMd5,categoryStr);
      //ObjectMapper mapper=new ObjectMapper();
      //String categoryStr=mapper.writeValueAsString(user_category);
      //shardedJedis.hset("user_categories",uid,categoryStr);
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

  //query interface for users.map<uid,Map<url,count>>
  @Override
  public void submitBatch(Map<String, Map<String, Integer>> users) {
    logger.info("submit Batch");
    ShardedJedis shardedJedis=null;
    boolean successful=true;
    try{
      shardedJedis=redisShardedPoolManager.borrowShardedJedis();
      Map<String,Map<String,Double>> user_categories=inferenceBatch(shardedJedis,users);
      ObjectMapper mapper=new ObjectMapper();
      for(Map.Entry<String,Map<String,Double>> entry: user_categories.entrySet()){
        Map<String,Double> user=entry.getValue();
        String uid=entry.getKey();
        Map<String,Integer> spUser=getSimplifiedCategories(user);
        StringBuilder resultBuilder=new StringBuilder();
        for(Map.Entry<String,Integer> entry1: spUser.entrySet()){
            resultBuilder.append(entry1.getKey());
            if(entry1.getValue()<10)
              resultBuilder.append("0"+entry1.getValue());
             else
              resultBuilder.append(entry1.getValue());
        }
        String categoryStr=resultBuilder.toString();
        String uidMd5= BDMD5.getInstance().toMD5(uid);
        logger.info("uidMd5 "+uidMd5);
        shardedJedis.set(uidMd5,categoryStr);
        //String categoryStr=mapper.writeValueAsString(user);
        //shardedJedis.hset("user_categories",uid,categoryStr);
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
  //get category:probability for the user. category_probability*url_frequent*url_probability
  protected Map<String,Double>  inference(ShardedJedis shardedJedis,Map<String, Integer> user) {
    double categories_weight = 0;
    Map<String, Double> user_category = new HashMap<String, Double>();
    for (String url : user.keySet()) {
      if (this.url_category.containsKey(url)) {
        String category = this.url_category.get(url);
        String category_probability_str=shardedJedis.hget(category_probability_key, category);
        String url_probability_str=shardedJedis.hget(url_probability_key,url);
        try{
        double category_probability = Double.valueOf(category_probability_str);
        double url_probability = Double.valueOf(url_probability_str);
        int url_frequent = user.get(url);
        double category_weight = category_probability * url_frequent * url_probability;
        //System.out.println("url: url"+" ||| url_frequent: "+url_frequent+" ||| url_probability: "+url_probability
        //                   +" ||| category: "+category+" ||| category_probability: "+category_probability);
        if (user_category.containsKey(category)) {
          user_category.put(category, user_category.get(category) + category_weight);
        } else {
          user_category.put(category, category_weight);
        }
        categories_weight += category_weight;
      }catch (Exception e){
          e.printStackTrace();
          System.out.println("category: " + category + " url: " + url);
          System.out.println(category_probability_str + "   " + url_probability_str);
          continue;
        }
      }
    }

    for (String category : user_category.keySet()) {
      user_category.put(category, user_category.get(category) / categories_weight);
    }

    return user_category;
  }
  // get Map<uid,Map<category,probability>>
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
  // get simplified categories(three categories a,b,z) from original categories(about 20 categories)
  protected Map<String,Integer> getSimplifiedCategories(Map<String,Double> user_category){
    Map<String,Double> spCategories=new HashMap<String,Double>();
    Configuration conf= Config.createConfig("/category_map.properties", Config.ConfigFormat.properties);
    logger.info("get keys in conf");
    Iterator<String> keys=conf.getKeys();

    while(keys.hasNext()){
      logger.info("key: "+keys.next());
    }
    logger.info("all keys list");
    for(Map.Entry<String,Double> entry: user_category.entrySet()){
      String spCategory=getSpCategory(conf,entry.getKey());
      if(spCategory==null){
        //logger.info("origCategory "+entry.getKey()+" spCategory is null");
        spCategory="z";
      }else {
        logger.info("origCategory "+entry.getKey());
      }
      Double probability=spCategories.get(spCategory);
      if(probability==null){
        probability=new Double(0);
      }
      probability+=entry.getValue();
      spCategories.put(spCategory,probability);
    }
    Map<String,Integer> spCategoryProbs=new HashMap<String,Integer>();
    int sum=0;
    for(Map.Entry<String,Double> entry: spCategories.entrySet()){
       if(!entry.getKey().equals("z")){
         Integer prob=(int)(entry.getValue()*100);
         spCategoryProbs.put(entry.getKey(),prob);
         sum+=prob;
       }
    }
    spCategoryProbs.put("z",100-sum);
    return spCategoryProbs;
  }

  private String getSpCategory(Configuration conf,String category){
    return conf.getString(category);
  }



}
