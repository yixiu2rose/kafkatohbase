package com.yonyou.jsonBean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/* 
 * @author WeiLiPeng
 *
 * @date 2017年11月30日
 */
public class DtunnelYGBean implements Serializable {

	private static final long serialVersionUID = 8565195522646005050L;

//	private List<DtunnelYGMessageBean> message;
	
	private ArrayList<HashMap<String,String>> message;
	
	private String bizcode;

	private String timestamp;

	private String host;

	/*public List<DtunnelYGMessageBean> getMessage() {
		return message;
	}

	public void setMessage(List<DtunnelYGMessageBean> message) {
		this.message = message;
	}*/

	
	
	public String getBizcode() {
		return bizcode;
	}

	public ArrayList<HashMap<String, String>> getMessage() {
		return message;
	}

	public void setMessage(ArrayList<HashMap<String, String>> message) {
		this.message = message;
	}

	public void setBizcode(String bizcode) {
		this.bizcode = bizcode;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}
}
