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
@XmlRootElement
public class Bizcodes {

	@XmlElement(name = "bizcode")  
	List<Bizcode> bizcodes;

	
	public List<Bizcode> getBizcodes() {
		return bizcodes;
	}

	@XmlTransient
	public void setBizcodes(List<Bizcode> bizcodes) {
		this.bizcodes = bizcodes;
	}
	
	
}

