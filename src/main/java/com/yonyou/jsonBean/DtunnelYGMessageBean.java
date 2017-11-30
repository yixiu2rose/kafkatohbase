package com.yonyou.jsonBean;

import java.io.Serializable;

/* 
 * @author WeiLiPeng
 *
 * @date 2017年11月30日
 */
public class DtunnelYGMessageBean implements Serializable{
	
	private static final long serialVersionUID = 7610483940361355043L;

	private String age;
	
	private String name;
	
	private String opration;
	
	private String url;

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getOpration() {
		return opration;
	}

	public void setOpration(String opration) {
		this.opration = opration;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	
	
}

