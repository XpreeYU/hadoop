package com.sun.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase CRUD Operations
 * @author yujiansong
 *	  @date 2017年11月19日
 */
public class HBaseOpreation {

    public static HTable getHTableByTableName(String tableName) throws Exception{
        // Get instance of Default Configuration
        Configuration configuration = HBaseConfiguration.create();
        
        //get table instance 
        HTable table = new HTable(configuration, tableName);
        
        return table;
    }
    
    
    public static void main(String[] args) throws Exception  {
        String tableName  = "user"; //default.user   /hbase:meta

        HTable table = getHTableByTableName(tableName);
        
        // Create get with rowKey
        Get get = new Get(Bytes.toBytes("10002")); //"10002".toBytes()
    
        //Get data
        Result result = table.get(get);
    
        // key : roekey + cf + c + version
        // value : value
        for (Cell cell : result.rawCells()) {
            System.out.println(
                    Bytes.toString(CellUtil.cloneFamily(cell)) + ":" //
                  + Bytes.toString(CellUtil.cloneQualifier(cell)) + " ->"
                  + Bytes.toString(CellUtil.cloneValue(cell)) + ":"
            );
        }
        
        //Table Close
        table.close();
    }

    

}
