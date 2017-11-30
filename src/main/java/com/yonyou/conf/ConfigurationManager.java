package com.yonyou.conf;

import java.io.InputStream;
import java.util.Properties;

/**
 * 
 * @author WeiLiPeng
 *
 * @date 2017年11月30日
 */
public class ConfigurationManager {
	
    private static Properties pro = new Properties();

    static {
        try {
            InputStream inputStream = ConfigurationManager.class
                    .getClassLoader()
                    .getResourceAsStream("config.properties");
            pro.load(inputStream);
        } catch (Exception e) {

        }
    }

    public static String getProperty(String key) {
        return pro.getProperty(key);
    }

    public static Integer getInteger(String key) {
        return Integer.valueOf(pro.getProperty(key));
    }

    public static Boolean getBoolean(String key) {
        return Boolean.valueOf(pro.getProperty(key));
    }

}
