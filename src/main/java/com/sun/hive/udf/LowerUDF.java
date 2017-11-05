package com.sun.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 
 * @author yujiansong
 *	  @date 2017年9月28日
 */
public class LowerUDF extends UDF{

    public Text evaluate(Text str){
        //validate
        if (null == str.toString()) {
            return null;
        }
        
        //lower
        return new Text(str.toString().toLowerCase());
    }
    
    public static void main(String[] args) {
        System.out.println(new LowerUDF().evaluate(new Text("HIVE")));
    }
}
