package com.elex.bigdata.user_category.service;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/5/14
 * Time: 2:40 PM
 * To change this template use File | Settings | File Templates.
 */
public interface Submit {
  public void submit(String uid, Map<String, Integer> user);
  public void submitBatch(Map<String, Map<String, Integer>> users);
}
