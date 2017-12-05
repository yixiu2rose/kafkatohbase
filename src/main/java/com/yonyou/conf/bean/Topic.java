package com.yonyou.conf.bean;

import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/* 
 * @author WeiLiPeng
 *
 * @date 2017年11月30日
 */
@XmlRootElement
public class Topic {

	String name;
	
	String group;
	
	int threadNum;

	@XmlElement(name = "bizcode")  
	List<Bizcode> bizcodes;
	

	public String getName() {
		return name;
	}

	@XmlAttribute
	public void setName(String name) {
		this.name = name;
	}

	public List<Bizcode> getBizcodes() {
		return bizcodes;
	}

	@XmlTransient
	public void setBizcodes(List<Bizcode> bizcodes) {
		this.bizcodes = bizcodes;
	}

	public String getGroup() {
		return group;
	}

	@XmlAttribute
	public void setGroup(String group) {
		this.group = group;
	}

	public int getThreadNum() {
		return threadNum;
	}

	@XmlAttribute
	public void setThreadNum(int threadNum) {
		this.threadNum = threadNum;
	}

	
	
}
