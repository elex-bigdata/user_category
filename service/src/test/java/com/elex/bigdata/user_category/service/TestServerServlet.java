package com.elex.bigdata.user_category.service;

import com.elex.bigdata.conf.Config;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/7/14
 * Time: 2:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestServerServlet {
  private static Logger logger=Logger.getLogger(TestServerServlet.class);
  @Test
  public void test(){
    QueryServerServlet serverServlet=new QueryServerServlet();
    System.out.println("hh");
  }
  @Test
  public void test1(){
    Configuration conf= Config.createConfig("/category_map.properties", Config.ConfigFormat.properties);
    logger.info("get keys in conf");
    Iterator<String> keys=conf.getKeys();
    while(keys.hasNext()){
      logger.info("key: "+keys.next());
    }
    logger.info("all keys list");
    logger.info(Integer.toHexString(6));
  }
}
