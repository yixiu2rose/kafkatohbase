package com.yonyou.kafka;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yonyou.conf.bean.Bizcode;
import com.yonyou.conf.bean.Topic;
import com.yonyou.conf.bean.Topics;

/* 
 * @author WeiLiPeng
 *
 * @date 2017年11月30日
 */
public class ConsumerHandeler {

	private static final Logger log = LoggerFactory.getLogger(ConsumerHandeler.class.getName());
//	private static final Logger log = LoggerFactory.getLogger(ConsumerHandeler.class);

	
	public static Topics gainTopics() {
		InputStream file = ConsumerHandeler.class.getClassLoader().getResourceAsStream("topic_map_hbase.xml");
		JAXBContext jaxbContext = null;
		Topics topics = null;
		try {
			jaxbContext = JAXBContext.newInstance(Topics.class);
			Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
			topics = (Topics) jaxbUnmarshaller.unmarshal(file);
			List<Topic> topicList = topics.getTopicList();
			for(Topic topic :topicList) {
				List<Bizcode> bizcodes = topic.getBizcodes();
				System.out.println("topic name:"+topic.getName()+" group:"+topic.getGroup()+"  thread num:"+topic.getThreadNum());
				for(Bizcode biz:bizcodes) {
					System.out.println("code:"+biz.getCode()+"  table:"+biz.getHbaseTableName());
				}
			}
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		return topics;
	}
	
	public static Map<String,Bizcode> gainTopicBiz(String topicName,Topics topics) {
		List<Topic> topicList = topics.getTopicList();
		Map<String,Bizcode> res = new HashMap<String,Bizcode>();
		for(Topic top : topicList) {
			String name = top.getName();
			if(topicName.equals(name)) {
				List<Bizcode> bizcodes = top.getBizcodes();
				for(Bizcode biz :bizcodes) {
					res.put(biz.getCode(), biz);
				}
			}
		}
		return res;
	}
	
	
}
