package com.elex.bigdata.user_category.train;

import com.elex.bigdata.conf.Config;
import com.elex.bigdata.ro.BasicRedisShardedPoolManager;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import redis.clients.jedis.ShardedJedis;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.elex.bigdata.user_category.train.RedisConstans.*;


/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/4/14
 * Time: 6:25 PM
 * To change this template use File | Settings | File Templates.
 */
public class BayesTrainer {
  private Map<String, String> url_category; //url 类型映射
  private Map<String, Long> url_frequent; //url 频率
  private Map<String, Long> category_frequent; //类型频率
  private long click_frequent;//点击频率
  private Map<String, Double> url_probability; //url 概率
  private Map<String, Double> category_probability; //类型概率
  private static Logger logger = Logger.getLogger(BayesTrainer.class);
  private BasicRedisShardedPoolManager manager = null;
  private Configuration configuration;

  public BayesTrainer() {
    this.url_category = new HashMap<String, String>();
    this.url_frequent = new HashMap<String, Long>();
    this.category_frequent = new HashMap<String, Long>();
    this.click_frequent = 0;
    this.url_probability = new HashMap<String, Double>();
    this.category_probability = new HashMap<String, Double>();
    this.manager = new BasicRedisShardedPoolManager("BayesTrainer", "/redis.site.properties");
    this.configuration = Config.createConfig("/user_category.properties", Config.ConfigFormat.properties);
  }

  public static void main(String[] args) {
    if(args.length<3)
    {
      logger.info("args length should be greater then 3. 0: click_log_file 1:url_category_file 2:restoreFrequencyWay");
      return;
    }
    String click_log_file = args[0];
    String url_category_file=args[1];
    String restoreWay = args[2];
    BayesTrainer bayesTrainer = new BayesTrainer();
    if (restoreWay.equals("redis")) {
      bayesTrainer.trainFromRedis(click_log_file,url_category_file);
    } else if (restoreWay.equals("disk")) {
        String url_frequent_file = bayesTrainer.getDefaultUrlFrequentFile(),
        category_frequent_file = bayesTrainer.getDefaultCategoryFrequentFile();
        if (args.length > 3) {
          url_frequent_file = args[3];
          if (args.length > 4) {
            category_frequent_file = args[4];
          }
        }
      bayesTrainer.trainFromFile(click_log_file, url_category_file, url_frequent_file, category_frequent_file);
    } else {
      logger.info("args[1] should be disk or redis but it is " + args[1]);
    }
    logger.info("train completed");
  }
  /*
     not always in memory.runs once a day.
     indicate in the program args that get url frequent and category frequent from disk or redis.

     the main function is train():
        according to url_category_file , click_log_file and url_frequent,category_frequent in redis to produce url_frequent and category_frequent.
             for url in click_log_file:
                get the url frequent and increase it by frequent in click_log_file;
                (if the url frequent does not lives in memory then get in redis)
                get the category frequent and increase it by frequent in click_log_file;
        then according to url_frequent and category_frequent to produce category_probability and url_probability
             for category in url_category.values()(should save it in a list):
                if category_frequent does not contains key category
                then
                   get the frequent from redis.
        then
             put url_frequent and category_frequent to redis .
             put url_probability and category_probability to redis.
             put url_frequent and category_frequent to disk.
   */
  /*
  load url_category file from disk
  url_category:
     every line composed of type and urls .they are seperated by " ";
     Notícias odia.ig.com.br www.abril.com.br
     produce map which has url as key and category ad map.
  */
  private void load_url_category(String url_category_file) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(url_category_file)), "UTF8"));
    String line = "";
    while ((line = reader.readLine()) != null) {
      String[] tokens = line.split(" ");
      if (tokens.length < 2) {
        continue;
      }
      String category = tokens[0];
      for (int i = 1; i < tokens.length; i++) {
        String url = tokens[i];
        this.url_category.put(url, category);
      }
    }
    reader.close();
  }
  // load url_category from redis
  private void load_url_category(ShardedJedis shardedJedis) {
    this.url_category = shardedJedis.hgetAll("url_category");
  }
  // load category frequent from redis .
  // stored a hashMap in redis "category_frequent".
  private void load_category_frequent(ShardedJedis shardedJedis) {
    Map<String, String> map = shardedJedis.hgetAll("category_frequent");
    logger.info("load category_frequent from redis");
    for (Map.Entry<String, String> entry : map.entrySet()) {
      this.category_frequent.put(entry.getKey(), Long.valueOf(entry.getValue()));
      logger.info("category: "+entry.getKey()+" value: "+entry.getValue());
    }
  }
  /*
    load category_frequent from disk file
    each line composed of category and frequent seperated by "\t"
    put category:frequent to map
  */
  private void load_category_frequent(String category_frequent_file) {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(category_frequent_file)), "UTF8"));
      String line = "";
      while ((line = reader.readLine()) != null) {
        String[] tokens = line.split("\t");
        if (tokens.length < 2) {
          continue;
        }
        String category = tokens[0];
        String frequent = tokens[1];
        this.category_frequent.put(category, Long.valueOf(frequent));
      }
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  /*
    load url_frequent from redis
  */
  private void load_url_frequent(ShardedJedis shardedJedis) {
    Map<String, String> map = shardedJedis.hgetAll("url_frequent");
    for (Map.Entry<String, String> entry : map.entrySet()) {
      this.url_frequent.put(entry.getKey(), Long.valueOf(entry.getValue()));
    }
  }
  /*
    load url_frequent from disk file
    each line composed of url and frequent seperated by "\t"
    put category:frequent to map
   */
  private void load_url_frequent(String url_frequent_file) {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(url_frequent_file)), "UTF8"));
      String line = "";
      while ((line = reader.readLine()) != null) {
        String[] tokens = line.split("\t");
        if (tokens.length < 2) {
          continue;
        }
        String url = tokens[0];
        String frequent = tokens[1];
        this.category_frequent.put(url, Long.valueOf(frequent));
      }
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  /*
     put url_probability , category_probability, url_frequent,category_frequent to redis
   */
  private void putToRedis(ShardedJedis shardedJedis) {
    Map<String, String> url_probability_map = new HashMap<String, String>();
    for (Map.Entry<String, Double> entry : url_probability.entrySet()) {
      url_probability_map.put(entry.getKey(), String.valueOf(entry.getValue()));
    }
    //shardedJedis.del(url_probability_key);
    if (url_probability_map.size() != 0)
      shardedJedis.hmset(url_probability_key, url_probability_map);
    Map<String, String> category_probability_map = new HashMap<String, String>();
    for (Map.Entry<String, Double> entry : category_probability.entrySet()) {
      category_probability_map.put(entry.getKey(), String.valueOf(entry.getValue()));
    }
    if (category_probability_map.size() != 0)
      shardedJedis.hmset(category_probability_key, category_probability_map);
    Map<String, String> url_frequent_map = new HashMap<String, String>();
    for (Map.Entry<String, Long> entry : url_frequent.entrySet()) {
      url_frequent_map.put(entry.getKey(), String.valueOf(entry.getValue()));
    }
    shardedJedis.del(url_frequent_key);
    if (url_frequent_map.size() != 0)
      shardedJedis.hmset(url_frequent_key, url_frequent_map);
    Map<String, String> category_frequent_map = new HashMap<String, String>();
    for (Map.Entry<String, Long> entry : category_frequent.entrySet()) {
      category_frequent_map.put(entry.getKey(), String.valueOf(entry.getValue()));
    }
    shardedJedis.del(category_frequent_key);
    if (category_frequent_map.size() != 0)
      shardedJedis.hmset(category_frequent_key, category_frequent_map);
    if (url_category.size() != 0)
      shardedJedis.hmset(url_category_key, url_category);
  }
  /*
     put url_frequent,category_frequent to disk to save key infos.
   */
  private void putToDisk() throws IOException {
    String url_frequent_file = getUrlFrequentFile();
    BufferedWriter url_frequent_writer = new BufferedWriter(new FileWriter(url_frequent_file));
    for (Map.Entry<String, Long> entry : url_frequent.entrySet()) {
      String line = entry.getKey() + "\t" + String.valueOf(entry.getValue());
      url_frequent_writer.write(line);
      url_frequent_writer.newLine();
    }
    url_frequent_writer.flush();
    url_frequent_writer.close();
    String category_frequent_file = getCategoryFrequentFile();
    BufferedWriter category_frequent_writer = new BufferedWriter(new FileWriter(category_frequent_file));
    for (Map.Entry<String, Long> entry : category_frequent.entrySet()) {
      String line = entry.getKey() + "\t" + String.valueOf(entry.getValue());
      category_frequent_writer.write(line);
      category_frequent_writer.newLine();
    }
    category_frequent_writer.flush();
    category_frequent_writer.close();
  }

  private String getUrlFrequentFile() {
    //todo rename file and get new file
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHH");
    String timeStr = format.format(new Date());
    return getDefaultUrlFrequentFile() + timeStr;
  }

  private String getCategoryFrequentFile() {
    //todo rename file and get new file
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHH");
    String timeStr = format.format(new Date());
    return getDefaultCategoryFrequentFile() + timeStr;
  }

  protected String getDefaultUrlFrequentFile() {
    return configuration.getString("url_frequent_file");
  }

  protected String getDefaultCategoryFrequentFile() {
    return configuration.getString("category_frequent_file");
  }

  protected String getDefaultUrlcategoryFile() {
    return configuration.getString("url_category_file");
  }
  // get url_category and category_frequent from redis to train.
  // input is click_log and url_category files
  public void trainFromRedis(String click_log_file,String url_category_file) {
    ShardedJedis shardedJedis = null;
    boolean successful = true;
    try {
      shardedJedis = manager.borrowShardedJedis();
      load_url_category(url_category_file);
      load_category_frequent(shardedJedis);
      load_url_frequent(shardedJedis);
      load_click_log(click_log_file);
      train_model();
      putToRedis(shardedJedis);
      putToDisk();
    } catch (Exception e) {
      successful = false;
      e.printStackTrace();
    } finally {
      if (successful)
        manager.returnShardedJedis(shardedJedis);
      else
        manager.returnBrokenShardedJedis(shardedJedis);
    }
  }
  //get click_log("url:count"),url_category,url_frequent,category_frequent from disk file to train
  // the train is incremental.
  public void trainFromFile(String click_log_file, String url_category_file, String url_frequent_file, String category_frequent_file) {
    ShardedJedis shardedJedis = null;
    boolean successful = true;
    try {
      load_url_category(url_category_file);
      load_url_frequent(url_frequent_file);
      load_category_frequent(category_frequent_file);
      load_click_log(click_log_file);
      train_model();
      shardedJedis = manager.borrowShardedJedis();
      putToRedis(shardedJedis);
      putToDisk();
    } catch (Exception e) {
      successful = false;
      e.printStackTrace();
    } finally {
      if (successful)
        manager.returnShardedJedis(shardedJedis);
      else
        manager.returnBrokenShardedJedis(shardedJedis);
    }
  }

   //load click log from disk.
  //each line is composed of url and frequent.
  private void load_click_log(String click_log_file) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(click_log_file)), "UTF8"));
    String line = "";
    String[] fields = new String[2];
    String url = "";
    Long frequent = new Long(0);
    while ((line = reader.readLine()) != null) {
      try {
        fields = line.split("\t");
        url = fields[0];
        frequent = Long.parseLong(fields[1]);
      } catch (Exception ex) {
        continue;
      }
      //System.out.println(line);

      if (this.url_category.containsKey(url)) {
        String category = this.url_category.get(url);
        if (!this.url_frequent.containsKey(url)) {
          this.url_frequent.put(url, frequent);
        } else {
          this.url_frequent.put(url, this.url_frequent.get(url) + frequent);
        }
        if (!this.category_frequent.containsKey(category)) {
          this.category_frequent.put(category, frequent);
        } else {
          this.category_frequent.put(category, this.category_frequent.get(category) + frequent);
        }
        this.click_frequent += frequent;
      }
    }
    reader.close();
  }
  // according to category_frequent and url_frequent to produce category_probability and url_probability
  private void train_model() {
    for (String category : this.category_frequent.keySet()) {
      long category_frequent = this.category_frequent.get(category);
      this.category_probability.put(category, 1.0 * category_frequent / this.click_frequent);
    }
    for (String url : this.url_frequent.keySet()) {
      String category = this.url_category.get(url);
      try{
      long category_frequent = this.category_frequent.get(category);
      long url_frequent = this.url_frequent.get(url);
      this.url_probability.put(url, 1.0 * url_frequent / category_frequent);
      }catch (Exception e){
        e.printStackTrace();
        logger.info("category "+category+" url "+url);
      }
    }
  }


}
