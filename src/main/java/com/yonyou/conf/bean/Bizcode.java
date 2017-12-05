package com.yonyou.conf.bean;

import javax.xml.bind.annotation.XmlAttribute;

/* 
 * @author WeiLiPeng
 *
 * @date 2017年11月30日
 */
public class Bizcode {

	String code;

	String hbaseTableName;

	String family;
	
//	String topicName;
	
	

	/*public String getTopicName() {
		return topicName;
	}
	
	@XmlAttribute
	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}*/

	public String getCode() {
		return code;
	}

	@XmlAttribute
	public void setCode(String code) {
		this.code = code;
	}

	public String getHbaseTableName() {
		return hbaseTableName;
	}

	@XmlAttribute
	public void setHbaseTableName(String hbaseTableName) {
		this.hbaseTableName = hbaseTableName;
	}

	public String getFamily() {
		return family;
	}

	@XmlAttribute
	public void setFamily(String family) {
		this.family = family;
	}
}
