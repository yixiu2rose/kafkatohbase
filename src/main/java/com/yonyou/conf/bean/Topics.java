package com.yonyou.conf.bean;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/* 
 * @author WeiLiPeng
 *
 * @date 2017年11月30日
 */
@XmlRootElement(name = "topics")  
public class Topics {

	@XmlElement(name = "topic")  
	List<Topic> topicList;

	@XmlTransient
	public List<Topic> getTopicList() {
		return topicList;
	}

	public void setTopicList(List<Topic> topicList) {
		this.topicList = topicList;
	}
	
	
}
