package com.elex.bigdata.user_category.train;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 3/7/14
 * Time: 11:03 AM
 * To change this template use File | Settings | File Templates.
 */
public class ChangeUrlCategory {
  private String head="http://",head1="https://",tail="/";
  private String url_category_file="/data/log/user_category/mirrors/url_category";
  @Test
  public void simplifyUrl() throws IOException {
    BufferedReader reader=new BufferedReader(new FileReader(url_category_file));
    Map<String,String> categoryUrls=new HashMap<String,String>();
    String line="";
    while((line=reader.readLine())!=null){
       String[] fields=line.split(" ");
       if(fields.length<2)
         continue;
       StringBuilder simplifiedUrls=new StringBuilder();
       String category=fields[0];
       for(int i=1;i<fields.length;i++){
         String url=fields[i];
         url.trim();
         if(url.startsWith(head))
           url=url.substring(7);
         else if(url.startsWith(head1))
           url=url.substring(head1.length());
         if(url.endsWith(tail))
           url=url.substring(0,url.length()-1);
         simplifiedUrls.append(url);
         simplifiedUrls.append(" ");
       }
       categoryUrls.put(category,simplifiedUrls.toString());
    }
    reader.close();
    BufferedWriter writer=new BufferedWriter(new FileWriter(url_category_file+"_simple"));
    for(Map.Entry<String,String> entry:categoryUrls.entrySet()){
      writer.write(entry.getKey()+" "+entry.getValue());
      writer.newLine();
    }
    writer.flush();
    writer.close();
    File origFile=new File(url_category_file);
    origFile.renameTo(new File(url_category_file+"_orig"));
    File destFile=new File((url_category_file+"_simple"));
    destFile.renameTo(new File(url_category_file));
  }
}
