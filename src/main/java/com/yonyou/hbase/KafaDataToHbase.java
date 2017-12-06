package com.yonyou.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

/*
 * @author WeiLiPeng
 * @date 2017年11月30日
 */
public class KafaDataToHbase {

    public static void insetData(String tableNameStr, String family, ArrayList<HashMap<String, String>> messages) {

        TableName tableName = TableName.valueOf(tableNameStr);
        Table table = null;
        Connection connection = HBaseConnectionPool.getInstance().getConnection();
        try {
            table = connection.getTable(tableName);
            // String family = "yg_message";
            for (HashMap<String, String> message : messages) {
                String rowKey = UUID.randomUUID().toString();
                Put put = new Put(rowKey.getBytes());
                for (Map.Entry<String, String> entry : message.entrySet()) {
                    String column = entry.getKey();
                    String value = entry.getValue();
                    put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
                }
                table.put(put);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != table) try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            HBaseConnectionPool.getInstance().releaseConnection(connection);
        }

    }

    public static void createTable(String hbaseTableName, String family) {
        TableName tableName = TableName.valueOf(hbaseTableName);
        Connection connection = HBaseConnectionPool.getInstance().getConnection();
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            if (admin.tableExists(tableName)) {
            } else {
                HTableDescriptor tableDesc = new HTableDescriptor(tableName);
                tableDesc.addFamily(new HColumnDescriptor(family).setCompactionCompressionType(Algorithm.LZO));
                admin.createTable(tableDesc);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
