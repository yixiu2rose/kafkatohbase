package com.yonyou.kafka;

import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yonyou.conf.bean.Bizcode;
import com.yonyou.conf.bean.Bizcodes;

/* 
 * @author WeiLiPeng
 *
 * @date 2017年11月30日
 */
public class ConsumerHandeler {

	private static final Logger log = LoggerFactory.getLogger(ConsumerHandeler.class.getName());
//	private static final Logger log = LoggerFactory.getLogger(ConsumerHandeler.class);

	public static Map<String, Bizcode> gainCodeMapInfo() {
		Map<String, Bizcode> map = new HashMap<String, Bizcode>();
		InputStream file = ConsumerHandeler.class.getClassLoader().getResourceAsStream("topic_map_hbase.xml");
		JAXBContext jaxbContext;
		try {
			jaxbContext = JAXBContext.newInstance(Bizcodes.class);
			Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
			Bizcodes bizcodes = (Bizcodes) jaxbUnmarshaller.unmarshal(file);
			for (Bizcode biz : bizcodes.getBizcodes()) {
				String name = biz.getTopicName();
				String code = biz.getCode();
				log.info("=============topic name:" + name + "   family:" + biz.getFamily() + "  tableName:"
						+ biz.getHbaseTableName() + "  bizcode:" + code);
				map.put(code, biz);
			}
		} catch (JAXBException e) {
			e.printStackTrace();
		}
		return map;
	}

	public static Set<String> gainTopics(Map<String, Bizcode> bizeCodeInfo) {
		Set<String> topics = new HashSet<String>();
		for (Map.Entry<String, Bizcode> entry : bizeCodeInfo.entrySet()) {
			Bizcode biz = entry.getValue();
			topics.add(biz.getTopicName());
		}
		return topics;
	}
}
