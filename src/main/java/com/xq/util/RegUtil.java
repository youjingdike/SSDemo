package com.xq.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xingqian
 * @正则表达式
 */
public class RegUtil {
    /**
     * 验证传入的字符串是否整个匹配正则表达式
     * @param regex 正则表达式
     * @param decStr 要匹配的字符串
     * @return 若匹配，则返回true;否则，返回false;
     */
    public static boolean validataAll(String regex, String decStr) {
        //表达式对象
        Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        //创建Matcher对象
        Matcher m = p.matcher(decStr);
        //是否完全匹配
        boolean b = m.matches();//该方法尝试将整个输入序列与该模式匹配
        return b;
    }
    
    /**
     * 验证传入的字符串是否整个匹配正则表达式数组里的任意一个
     * @param decStr 要匹配的字符串
     * @param regexs 正则表达式一个或多个
     * @return 若匹配，则返回true;否则，返回false;
     */
    public static boolean validataAllWithMulti(String decStr,String ...regexs) {
    	if(regexs==null) return false;
    	boolean b = false;
    	for (String regex : regexs) {
    		//表达式对象
    		Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
    		//创建Matcher对象
    		Matcher m = p.matcher(decStr);
    		//是否完全匹配
            b = m.matches();//该方法尝试将整个输入序列与该模式匹配
    		if(b) break;
		}
        return b;
    }
    
    /**
     * 验证传入的字符串是否有子字符串匹配正则表达式
     * @param regex 正则表达式
     * @param decStr 要匹配的字符串
     * @return 若匹配，则返回true;否则，返回false;
     */
    public static boolean validataSub(String regex, String decStr) {
        //表达式对象
        Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        //创建Matcher对象
        Matcher m = p.matcher(decStr);
        //是否匹配
        boolean b = m.find();//该方法扫描输入序列以查找与该模式匹配的下一个子序列。
        return b;
    }
    
    /**
     * 验证传入的字符串是否有子字符串匹配正则表达式数组里的任意一个
     * @param decStr 要匹配的字符串
     * @param regexs 正则表达式（一个或多个）
     * @return 若匹配，则返回true;否则，返回false;
     */
    public static boolean validataSubWithMulti(String decStr,String ...regexs) {
    	if(regexs==null) return false;
    	boolean b = false;
    	for (String regex : regexs) {
    		//表达式对象
    		Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
    		//创建Matcher对象
    		Matcher m = p.matcher(decStr);
    		//是否匹配
    		b = m.find();//该方法扫描输入序列以查找与该模式匹配的下一个子序列。
    		if(b) break;
		}
        return b;
    }
    
    /**
     * 给定字符串中是否有符合给定正则表达式的子字符串，返回匹配的第一个子字符串
     * @param regex：正则表达式
     * @param decStr：要匹配的字符串
     * @return :返回匹配的第一个字符串，若不匹配则null
     */
    public static String search(String regex, String decStr) {
        Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(decStr);
        String foundStr = "";
        try {
            if (m.find()) {
            	foundStr = m.group();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return foundStr;
    }
    
    /**
     * 返回给定字符串中匹配给定正则表达式所有子字符串
     * @param regex
     * @param decStr
     * @return List：返回所有匹配正则表达式的子字符串
     */
    public static List<String> searchSubStr(String regex,String decStr) {
        Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(decStr);  
        List<String> list = new ArrayList<String>();
        while(m.find()){
            list.add(m.group());
        }
        return list;
    }

    /**
     * 替换给定字符串中匹配正则表达式的子字符串
     * @param regex：正则表达式
     * @param decStr：所要匹配的字符串
     * @param replaceStr：将符合正则表达式的子串替换为该字符串
     * @return：返回替换以后新的字符串
     */
    public static String replaceAll(String regex,String decStr,String replaceStr) {
        Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(decStr);
        // 替换
        String newstring = m.replaceAll(replaceStr);
        return newstring;
    }

    /**
     * 替换给定字符串中匹配正则表达式的子字符串
     * @param regex：正则表达式
     * @param decStr：所要匹配的字符串
     * @param replaceStr：将符合正则表达式的子串替换为该字符串
     * @return：返回替换以后新的字符串
     */
    public static String replaceFirst(String regex,String decStr,String replaceStr) {
        Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(decStr);
        // 替换
        String newstring = m.replaceFirst(replaceStr);
        return newstring;
    }
    
    /**
     * 切分字符串
     * @param str
     * @param regex
     */
    public static String[] split(String str,String regex) {
        // 分割
        String [] strs = str.split(regex);
        for(int i=0;i<strs.length;i++) {
//System.out.println(i+":::::::"+strs[i].trim());
        }
        return strs;
    }   
    
    public static String[] split(String str,String regex,int limit) {
    	// 分割
    	String [] strs = str.split(regex,limit);
    	/*for(int i=0;i<strs.length;i++) {
    		System.out.println(i+":::::::"+strs[i].trim());
    	}*/
    	return strs;
    }   
    public static void main(String[] args) {
    	System.out.println(RegUtil.validataAll("\\d*", "334455"));
    	/*System.out.println(validataSub("[\\u4e00-\\u9fa5]","safdjsddsfsd1212,"));
    	System.out.println(validataSub("[^x00-xff]","safdjsddsfsd1212近距离看"));
    	split("@sfsa","@");
    	
    	System.out.println(replace("\\\\", "[\"u4e00-\"u9fa5]", ""));
    	System.out.println(replace("\"", "[\"u4e00-\"u9fa5]", ""));
    	System.out.println("\"sjfljdsf\"");*/
//    	System.out.println(validataSub(WMConstant.CANCEL_EXCE_FLAG,"详细：Key (order_code)=(1MT08412018011116370040) already exists"));
//    	System.out.println(validataSub(WMConstant.CANCEL_EXCE_FLAG,"详细：Key (third_order_code)=(1216181249124009074) already exists"));
    	String message = "";
//    	message = "=====================>> 【1sdf】【2】 【】返回给微信信息: <xml><return_code><![CDATA[SUCCESS]]></return_code><return_msg><![CDATA[OK]]></return_msg></xml>";
//    	message = "2018-01-24 04:44:07.282 [notifySpringScheduledExecutorFactoryBean-1] - [ERROR] com.tzx.cc.takeaway.unifiedreceiveorder.service.UnifiedReceiveOrderTaskServcieRunable(:64) 外卖新接单模式监听发生异常:Cannot get Jedis connection; nested exception is redis.clients.jedis.exceptions.JedisConnectionException: Could not get a resource from the pool";
//    	message = "2018-01-22 21:07:06.717 [notifySpringScheduledExecutorFactoryBean-1] - [ERROR] com.tzx.cc.takeaway.unifiedreceiveorder.service.UnifiedReceiveOrderTaskServcieRunable(:64) 外卖新接单模式监听发生异常:java.net.SocketTimeoutException: Read timed out; nested exception is redis.clients.jedis.exceptions.JedisConnectionException: java.net.SocketTimeoutException: Read timed out";
    	message = "存储d渠道数据到redis时发生异常JedisConnectionException";
//    	System.out.println(validataSubWithMulti(message, WMConstant.EXCP_TYPE_FLAG_6_2));
//    	System.out.println(validataSub(PayConstant.WX_CALLBACK_FLAG,message));
    	
    	/*List<String> searchSubStr = RegUtil.searchSubStr("【[^】]+】", message);
    	for (String string : searchSubStr) {
    		System.out.println("searchSubStr:"+string); 
        }
    	System.out.println(RegUtil.replaceAll("【", message, "@"));
    	System.out.println(RegUtil.replaceFirst("【", message, "@"));*/
    	
//    	String search = RegUtil.search(PayConstant.WX_CALLBACK_FLAG_1, message);
//    	System.out.println("search:"+search);
    	/*try {
			Document xmlDom = XMLUtil.getXMLDom(message,PayConstant.WX_CALLBACK_FLAG);
			String name = xmlDom.getRootElement().getName();
			System.out.println(name);
		} catch (DocumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}
}
