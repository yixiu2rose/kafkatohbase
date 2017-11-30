package com.yonyou.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yonyou.conf.ConfigurationManager;

/**
 * @Description: HBase链接池
 * @author jinfeng
 * @date 2017年10月23日
 */
public final class HBaseConnectionPool {
    private static final Logger log = LoggerFactory.getLogger(HBaseConnectionPool.class.getName());

    private final static String MIN_POOL_SIZE = "hbase.minPoolSize";
    private final static String MAX_POOL_SIZE = "hbase.maxPoolSize";

    private static HBaseConnectionPool instance;
    private static Object lock = new Object();
    private List<Connection> connections;
    private int maxPooSize;
    private Configuration hbaseConf;

    /**
     * 创建一个新的实例 HBaseConnectionPool.
     */
    private HBaseConnectionPool(Configuration conf, int minSize, int maxSize) {
        maxPooSize = maxSize;
        hbaseConf = conf;
        connections = new ArrayList<Connection>();
        for (int i = 0; i < minSize; ++i) {
            Connection hbaseConn = createConnection();
            connections.add(hbaseConn);
        }
    }

    /**
     * 创建一个HBase链接
     */
    private Connection createConnection() {
        try {
            return ConnectionFactory.createConnection(hbaseConf);
        } catch (IOException e) {
            log.error("创建HBase连接失败.", e);
            throw new RuntimeException("create hbase connection error.");
        }
    }

    /**
     * 设置链接池的初始链接
     */
    public static HBaseConnectionPool getInstance() {
        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    Configuration conf = HBaseConfiguration.create();
                    conf.set("hbase.zookeeper.quorum", ConfigurationManager.getProperty("hbase.zookeeper.quorum"));
                    conf.set("hbase.zookeeper.property.clientPort",
                            ConfigurationManager.getProperty("hbase.zookeeper.property.clientPort"));
                    conf.set("zookeeper.znode.parent", ConfigurationManager.getProperty("zookeeper.znode.parent"));
                    int minSize = conf.getInt(MIN_POOL_SIZE, 1);
                    int maxSize = conf.getInt(MAX_POOL_SIZE, 1);
                    instance = new HBaseConnectionPool(conf, minSize, maxSize);
                }
            }
        }
        return instance;
    }

    /**
     * 从链接池中获取链接，若链接池用尽则新建HBase Connection
     */
    public synchronized Connection getConnection() {
        if (connections.size() > 0) {
            return connections.remove(0);
        } else {
            return createConnection();
        }
    }

    /**
     * 回收链接到链接池中
     */
    public synchronized void releaseConnection(Connection conn) {
        if (connections.size() < maxPooSize) {
            connections.add(conn);
        } else {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (IOException e) {
                log.error("关闭HBase链接异常.", e);
            }
        }
    }

    /**
     * 关闭链接池中所有链接
     */
    public synchronized void closeAll() {
        try {
            for (Connection conn : connections) {
                if (conn != null) {
                    conn.close();
                }
            }
            // 清空所有被close的链接
            connections.clear();
        } catch (IOException e) {
            log.error("关闭HBase链接异常.", e);
        }
    }

}
