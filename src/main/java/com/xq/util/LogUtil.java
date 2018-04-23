package com.xq.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;

import com.xq.constant.LogConstant;
/**
 * 获取log信息属性的公共类
 * @author xingqian
 *
 */
public class LogUtil {
	
	/**
	 * 获取日志信息里面打出的时间
	 * @param logInfo
	 * @return
	 */
	public static String getLogTime(String logInfo) {
		if (StringUtils.isBlank(logInfo)) return "";
		
		String[] split = logInfo.split(LogConstant.LOG_TIME_FLAG,2);
		return split[0].trim().split("\\.")[0].trim();
	}
	
	/**
	 * 转化kafka信息里面的time(格式如：2018-01-05T08:46:30.777Z)为yyyy-MM-dd HH:mm:ss
	 * @param logInfo
	 * @return
	 * @throws ParseException
	 */
	public static String parseKafkaTime(String logInfo) throws ParseException {
		if (StringUtils.isBlank(logInfo)) return "";
		logInfo = logInfo.replaceAll("[A-Z]", " ");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = sdf.parse(logInfo.trim().split("\\.")[0].trim());
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+16"));
		String time = sdf.format(date);
		return time;
	}
	
	/**
	 * 获取日志里面的时间，如果日志里面解析不到时间，就读取日志发送的时间@timestamp
	 * @param logInfo
	 * @return
	 * @throws ParseException
	 */
	public static String getInfoTime(String logInfo,String kafka_time) throws ParseException {
		String time = getLogTime(logInfo);
		if (RegUtil.validataAll("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}",time)) {
			return time;
		} else {
			time = kafka_time==null?"":parseKafkaTime(kafka_time);
		}
		return time;
	}
	
	public static void main(String[] args) {
		String time = getLogTime("2018-01-09 13:38:22.734 [pool-12-thread-16] - [ERROR] com.tzx.cc.baidu.bo.imp.ThirdPartyOrderReceiver(:571)     插入数据库中的订单sql为:INSERT INTO cc_order_list");
//		System.out.println(time);
//		System.out.println(RegUtil.validataAll("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}",time));
		try {
			System.out.println(getInfoTime("2018-01-09 13:38:15.734 [pool-12-thread-16] - [ERROR] com.tzx.cc.baidu.bo.imp.ThirdPartyOrderReceiver(:571)     插入数据库中的订单sql为:INSERT INTO cc_order_list",time));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    
}
