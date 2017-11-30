package com.yonyou.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.yonyou.conf.ConfigurationManager;
import com.yonyou.conf.bean.Bizcode;
import com.yonyou.hbase.KafaDataToHbase;
import com.yonyou.jsonBean.DtunnelYGBean;

/*
 * @author WeiLiPeng
 * @date 2017年11月29日
 */
public class DtunnelConsumer extends Thread {

    // private static final Logger log =
    // LoggerFactory.getLogger(DtunnelConsumer.class);
    private static final Logger log = LoggerFactory.getLogger(DtunnelConsumer.class.getName());

    private String topicName;
    private Map<String, Bizcode> bizcodeMap;

    public DtunnelConsumer(String topicName, Map<String, Bizcode> bizcodeMap) {

        this.topicName = topicName;
        this.bizcodeMap = bizcodeMap;
    }

    public static Properties props = new Properties();

    static {
        props.put("bootstrap.servers", ConfigurationManager.getProperty("bootstrap.servers"));
        props.put("group.id", ConfigurationManager.getProperty("group.id"));
        props.put("enable.auto.commit", ConfigurationManager.getProperty("enable.auto.commit"));
        props.put("auto.commit.interval.ms", ConfigurationManager.getProperty("auto.commit.interval.ms"));
        props.put("session.timeout.ms", ConfigurationManager.getProperty("session.timeout.ms"));
        props.put("key.deserializer", ConfigurationManager.getProperty("key.deserializer"));
        props.put("value.deserializer", ConfigurationManager.getProperty("value.deserializer"));
    }

    @Override
    public void run() {
        log.info("========gain topic" + this.topicName + "============");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(DtunnelConsumer.props);
        consumer.subscribe(Arrays.asList(this.topicName));
        try {
            while (true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        DtunnelYGBean dtunnel = JSON.parseObject(record.value(), DtunnelYGBean.class);
                        if (null != dtunnel) {
                            String bizcode = dtunnel.getBizcode();
                            if (bizcodeMap.containsKey(bizcode)) {
                                Bizcode bizcodeInfo = bizcodeMap.get(bizcode);
                                ArrayList<HashMap<String, String>> messages = dtunnel.getMessage();
                                if (null != messages && messages.size() > 0) {
                                    KafaDataToHbase.insetData(bizcodeInfo.getHbaseTableName(), bizcodeInfo.getFamily(),
                                            messages);
                                }
                            }
                        }
                        log.info("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                    }
                } catch (JSONException e) {

                }
            }
        } finally {
            if (null != consumer) consumer.close();
        }
    }

}
