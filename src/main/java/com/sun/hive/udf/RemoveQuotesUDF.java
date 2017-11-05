package com.sun.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author yujiansong
 *	  @date 2017年9月28日
 */
public class RemoveQuotesUDF extends UDF{

    public Text evaluate(Text str){
        //validate
        if (null == str) {
            return new Text();
        }
        if (null == str.toString()) {
            return new Text();
        }
        
        //remove
        return new Text(str.toString().replaceAll("\"", ""));
    }
    
    public static void main(String[] args) {
        System.out.println(new RemoveQuotesUDF().evaluate(new Text("\"GET /course/view.php?id=27 HTTP/1.1\"")));
    }
}
