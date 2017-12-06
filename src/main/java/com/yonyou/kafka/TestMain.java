package com.yonyou.kafka;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.yonyou.conf.ConfigurationManager;
import com.yonyou.conf.bean.Bizcode;
import com.yonyou.conf.bean.Topic;
import com.yonyou.conf.bean.Topics;

/*
 * @author WeiLiPeng
 * @date 2017年11月30日
 */
public class TestMain {

    public static void main(String[] args) {

        ExecutorService executor = Executors.newFixedThreadPool(ConfigurationManager.getInteger("thread.pool.num"));
        // for (int i = 0; i < 10; i++) {
        // Runnable worker = new DtunnelConsumer("" + i);
        // executor.execute(worker);
        // }
        // executor.shutdown();
        // while (!executor.isTerminated()) {
        // }
        // System.out.println("Finished all threads");

        // File file = new File("D:/topic_map_hbase.xml");

		/*Map<String, Bizcode> gainCodeMapInfo = ConsumerHandeler.gainCodeMapInfo();

		Set<String> gainTopics = ConsumerHandeler.gainTopics(gainCodeMapInfo);*/
		
		Topics topics = ConsumerHandeler.gainTopics();
		List<Topic> topicList = topics.getTopicList();
		for (Topic topic : topicList) {
			int threadNum = topic.getThreadNum();
			 Map<String, Bizcode> gainTopicBiz = ConsumerHandeler.gainTopicBiz(topic.getName(), topics);
			for(int i=0;i<threadNum;i++) {
				Runnable worker = new DtunnelConsumer(topic.getName(),gainTopicBiz);
				executor.execute(worker);
			}
		}

        /*
         * InputStream file =
         * TestMain.class.getClassLoader().getResourceAsStream("topic_map_hbase.xml");
         * JAXBContext jaxbContext; try { jaxbContext =
         * JAXBContext.newInstance(Bizcodes.class); Unmarshaller jaxbUnmarshaller =
         * jaxbContext.createUnmarshaller(); Bizcodes bizcodes = (Bizcodes)
         * jaxbUnmarshaller.unmarshal(file); for (Bizcode biz : bizcodes.getBizcodes())
         * { String name = biz.getTopicName(); System.out.println("topic name:" + name);
         * System.out.println(biz.getFamily() + " " + biz.getHbaseTableName() + "  " +
         * biz.getCode());
         * // Runnable worker = new DtunnelConsumer(name, topic.getFamily()); //
         * executor.execute(worker); } // executor.shutdown(); // while
         * (!executor.isTerminated()) { // } System.out.println("Finished all threads");
         * } catch (JAXBException e) { e.printStackTrace(); }
         */

        // String kafkaJson =
        // "{\"message\":[{\"age\":22,\"name\":\"hengfeng\",\"opration\":\"login\",\"url\":\"www.hfbank.com.cn\"}],\"_bizcode\":\"YG10001\",\"_timestamp\":\"2017-11-2710:44:09\",\"_host\":\"192.168.199.187\"}";
        // DtunnelYGBean dtunnel = JSON.parseObject(kafkaJson, DtunnelYGBean.class);
        // System.out.println("getBizcode:" + dtunnel.getBizcode() + " getHost:" +
        // dtunnel.getHost() + " getTimestamp:"
        // + dtunnel.getTimestamp());
        // ArrayList<HashMap<String, String>> messages = dtunnel.getMessage();
        // for (Map<String, String> message : messages) {
        // for(Map.Entry<String, String> entry:message.entrySet()) {
        // System.out.println(entry.getKey()+" == "+entry.getValue());
        // }
        // }
        // JSON.toJavaObject(jsonString, HashMap.class);
        // System.out.println(jsonString);
    }
}
