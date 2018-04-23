package com.xq.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class JsonUtil {
	/**
     * 获取第三方平台推送日志信息中的json部分
     *
     * @param str
     * @param regex
     */
    private static String getJsonStrFromLogInfo(String str,String regex) {
    	if (StringUtils.isNotBlank(str)) {
    		String [] strs = str.split(regex);
    		if (strs.length==2) {
    			return strs[1].trim();
    		}
    	}
    	return null;
    }
    
    /**
     * 获取日志信息中的json部分的json对象
     *
     * @param str:日志信息
     * @param regex：切割符
     */
    public static JSONObject getJOBFromJsonStr(String str,String regex) {
    	String jsonStrInfo = getJsonStrFromLogInfo(str,regex);
    	return parseJOBFrmStr(jsonStrInfo);
    }
    
    /**
     * 获取日志信息中的json部分的json数组
     *
     * @param str:日志信息
     * @param regex：切割符
     */
    public static JSONArray getJobArrFromJsonStr(String str,String regex) {
    	String jsonStrInfo = getJsonStrFromLogInfo(str,regex);
    	return parseJobArrFrmStr(jsonStrInfo);
    }
    
    
    /**
     * 将字符串解析成JSONObject
     * @param jsonStr
     * @return
     */
    public static JSONObject parseJOBFrmStr(String jsonStr) {
    	try {
    		if (StringUtils.isNotBlank(jsonStr)) {
    			return JSONObject.parseObject(jsonStr);
    		}
    	} catch (Exception e) {
//			System.out.println(e.getMessage());
			e.printStackTrace();
		}
    	return null;
    }
    
    /**
     * 将字符串解析成JSONArray
     * @param jsonStr
     * @return
     */
    public static JSONArray parseJobArrFrmStr(String jsonStr) {
    	try {
    		if (StringUtils.isNotBlank(jsonStr)) {
    			return JSONObject.parseArray(jsonStr);
    		}
    	} catch (Exception e) {
//    		System.out.println(e.getMessage());
    		e.printStackTrace();
    	}
    	return null;
    }
    

	public static void main(String[] args) {
//    	System.out.println(new Date().getTime());
    	Map map = new HashMap();
    	map.put("ss", "sfasd");
    	System.out.println(map.size());
    	JSONObject js = parseJOBFrmStr("{\"name\":null}");
    	System.out.println(js.getString("name")==null);
    	
	}
}
