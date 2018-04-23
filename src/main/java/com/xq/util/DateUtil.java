package com.xq.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.commons.lang.StringUtils;
public class DateUtil {
	
	    // 默认日期格式
	    public static final String DATE_DEFAULT_FORMAT = "yyyy-MM-dd";

	    // 默认时间格式
	    public static final String DATETIME_DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

	    public static final String TIME_DEFAULT_FORMAT = "HH:mm:ss";

	    // 日期格式化
	    public static DateFormat dateFormat = null;

	    // 时间格式化
	    public static DateFormat dateTimeFormat = null;

	    public static DateFormat timeFormat = null;

	    public static Calendar gregorianCalendar = null;

	    static {
	        dateFormat = new SimpleDateFormat(DATE_DEFAULT_FORMAT);
	        dateTimeFormat = new SimpleDateFormat(DATETIME_DEFAULT_FORMAT);
	        timeFormat = new SimpleDateFormat(TIME_DEFAULT_FORMAT);
	        gregorianCalendar= new GregorianCalendar(1970,0,1);
	    }
	    /**
	     * 
	     * @param date
	     * @return
	     */
	    public static String formateNowDate(Date date){
			return dateFormat.format(date);
	    }
	    /**
	     * 
	     * @param date
	     * @return
	     */
	    public static String formateNowTimeDate(Date date){
			return dateTimeFormat.format(date);
	    }
	    
	     /**
	     * 日期格式化yyyy-MM-dd
	     * 
	     * @param date
	     * @return
	     */
	    public static Date formatDate(String date, String format) {
	        try {
	            return new SimpleDateFormat(format).parse(date);
	        } catch (ParseException e) {
	            e.printStackTrace();
	        }
	        return null;
	    }

	    /**
	     * 日期格式化yyyy-MM-dd
	     * 
	     * @param date
	     * @return
	     */
	    public static String getDateFormat(Date date) {
	        return dateFormat.format(date);
	    }

	    /**
	     * 日期格式化yyyy-MM-dd HH:mm:ss
	     * 
	     * @param date
	     * @return
	     */
	    public static String getDateTimeFormat(Date date) {
	        return dateTimeFormat.format(date);
	    }

	    /**
	     * 时间格式化
	     * 
	     * @param date
	     * @return HH:mm:ss
	     */
	    public static String getTimeFormat(Date date) {
	        return timeFormat.format(date);
	    }

	    /**
	     * 日期格式化
	     * 
	     * @param date
	     * @param 格式类型
	     * @return
	     */
	    public static String getDateFormat(Date date, String formatStr) {
	        if (StringUtils.isNotBlank(formatStr)) {
	            return new SimpleDateFormat(formatStr).format(date);
	        }
	        return null;
	    }

	    /**
	     * 日期格式化
	     * 
	     * @param date
	     * @return
	     */
	    public static Date getDateFormat(String date) {
	        try {
	            return dateFormat.parse(date);
	        } catch (ParseException e) {
	            e.printStackTrace();
	        }
	        return null;
	    }

	    /**
	     * 时间格式化
	     * 
	     * @param date
	     * @return
	     */
	    public static Date getDateTimeFormat(String date) {
	        try {
	            return dateTimeFormat.parse(date);
	        } catch (ParseException e) {
	            e.printStackTrace();
	        }
	        return null;
	    }

	    /**
	     * 获取当前日期(yyyy-MM-dd)
	     * 
	     * @param date
	     * @return
	     */
	    public static Date getNowDate() {
	        return DateUtil.getDateFormat(dateFormat.format(new Date()));
	    }

	    /**
	     * 获取当前日期星期一日期
	     * 
	     * @return date
	     */
	    public static Date getFirstDayOfWeek() {
	        gregorianCalendar.setFirstDayOfWeek(Calendar.MONDAY);
	        gregorianCalendar.setTime(new Date());
	        gregorianCalendar.set(Calendar.DAY_OF_WEEK, gregorianCalendar.getFirstDayOfWeek()); // Monday
	        return gregorianCalendar.getTime();
	    }

	    /**
	     * 获取当前日期星期日日期
	     * 
	     * @return date
	     */
	    public static Date getLastDayOfWeek() {
	        gregorianCalendar.setFirstDayOfWeek(Calendar.MONDAY);
	        gregorianCalendar.setTime(new Date());
	        gregorianCalendar.set(Calendar.DAY_OF_WEEK, gregorianCalendar.getFirstDayOfWeek() + 6); // Monday
	        return gregorianCalendar.getTime();
	    }

	    /**
	     * 获取日期星期一日期
	     * 
	     * @param 指定日期
	     * @return date
	     */
	    public static Date getFirstDayOfWeek(Date date) {
	        if (date == null) {
	            return null;
	        }
	        gregorianCalendar.setFirstDayOfWeek(Calendar.MONDAY);
	        gregorianCalendar.setTime(date);
	        gregorianCalendar.set(Calendar.DAY_OF_WEEK, gregorianCalendar.getFirstDayOfWeek()); // Monday
	        return gregorianCalendar.getTime();
	    }

	    /**
	     * 获取日期星期一日期
	     * 
	     * @param 指定日期
	     * @return date
	     */
	    public static Date getLastDayOfWeek(Date date) {
	        if (date == null) {
	            return null;
	        }
	        gregorianCalendar.setFirstDayOfWeek(Calendar.MONDAY);
	        gregorianCalendar.setTime(date);
	        gregorianCalendar.set(Calendar.DAY_OF_WEEK, gregorianCalendar.getFirstDayOfWeek() + 6); // Monday
	        return gregorianCalendar.getTime();
	    }

	    /**
	     * 获取当前月的第一天
	     * 
	     * @return date
	     */
	    public static Date getFirstDayOfMonth() {
	        gregorianCalendar.setTime(new Date());
	        gregorianCalendar.set(Calendar.DAY_OF_MONTH, 1);
	        return gregorianCalendar.getTime();
	    }

	    /**
	     * 获取当前月的最后一天
	     * 
	     * @return
	     */
	    public static Date getLastDayOfMonth() {
	        gregorianCalendar.setTime(new Date());
	        gregorianCalendar.set(Calendar.DAY_OF_MONTH, 1);
	        gregorianCalendar.add(Calendar.MONTH, 1);
	        gregorianCalendar.add(Calendar.DAY_OF_MONTH, -1);
	        return gregorianCalendar.getTime();
	    }

	    /**
	     * 获取指定月的第一天
	     * 
	     * @param date
	     * @return
	     */
	    public static Date getFirstDayOfMonth(Date date) {
	        gregorianCalendar.setTime(date);
	        gregorianCalendar.set(Calendar.DAY_OF_MONTH, 1);
	        return gregorianCalendar.getTime();
	    }

	    /**
	     * 获取指定月的最后一天
	     * 
	     * @param date
	     * @return
	     */
	    public static Date getLastDayOfMonth(Date date) {
	        gregorianCalendar.setTime(date);
	        gregorianCalendar.set(Calendar.DAY_OF_MONTH, 1);
	        gregorianCalendar.add(Calendar.MONTH, 1);
	        gregorianCalendar.add(Calendar.DAY_OF_MONTH, -1);
	        return gregorianCalendar.getTime();
	    }

	    /**
	     * 获取日期前一天
	     * 
	     * @param date
	     * @return
	     */
	    public static Date getDayBefore(Date date) {
	        gregorianCalendar.setTime(date);
	        int day = gregorianCalendar.get(Calendar.DATE);
	        gregorianCalendar.set(Calendar.DATE, day - 1);
	        return gregorianCalendar.getTime();
	    }
	    
	    /**
	     * 获取传入日期date的前或后bOrA天的日期
	     * @param date
	     * @param bOrA
	     * @return
	     */
	    public static String getDayBeforeOrAfter(Date date,int bOrA) {
	    	if (date!=null) {
	    		gregorianCalendar.setTime(date);
	    	} else {
	    		gregorianCalendar.setTime(new Date());
	    	}
	        int day = gregorianCalendar.get(Calendar.DATE);
	        gregorianCalendar.set(Calendar.DATE, day + bOrA);
//	        gregorianCalendar.set(Calendar.DAY_OF_MONTH, -1);
	        return dateFormat.format(gregorianCalendar.getTime());
	    }

	    /**
	     * 获取日期后一天
	     * 
	     * @param date
	     * @return
	     */
	    public static Date getDayAfter(Date date) {
	        gregorianCalendar.setTime(date);
	        int day = gregorianCalendar.get(Calendar.DATE);
	        gregorianCalendar.set(Calendar.DATE, day + 1);
	        return gregorianCalendar.getTime();
	    }

	    /**
	     * 获取当前年
	     * 
	     * @return
	     */
	    public static int getNowYear() {
	        Calendar d = Calendar.getInstance();
	        return d.get(Calendar.YEAR);
	    }

	    /**
	     * 获取当前月份
	     * 
	     * @return
	     */
	    public static int getNowMonth() {
	        Calendar d = Calendar.getInstance();
	        return d.get(Calendar.MONTH) + 1;
	    }

	    /**
	     * 获取当月天数
	     * 
	     * @return
	     */
	    public static int getNowMonthDay() {
	        Calendar d = Calendar.getInstance();
	        return d.getActualMaximum(Calendar.DATE);
	    }

	    /**
	     * 获取时间段的每一天
	     * 
	     * @param 开始日期
	     * @param 结算日期
	     * @return 日期列表
	     */
	    public static List<Date> getEveryDay(Date startDate, Date endDate) {
	        if (startDate == null || endDate == null) {
	            return null;
	        }
	        // 格式化日期(yy-MM-dd)
	        startDate = DateUtil.getDateFormat(DateUtil.getDateFormat(startDate));
	        endDate = DateUtil.getDateFormat(DateUtil.getDateFormat(endDate));
	        List<Date> dates = new ArrayList<Date>();
	        gregorianCalendar.setTime(startDate);
	        dates.add(gregorianCalendar.getTime());
	        while (gregorianCalendar.getTime().compareTo(endDate) < 0) {
	            // 加1天
	            gregorianCalendar.add(Calendar.DAY_OF_MONTH, 1);
	            dates.add(gregorianCalendar.getTime());
	        }
	        return dates;
	    }

	    /**
	     * 获取提前多少个月
	     * 
	     * @param monty
	     * @return
	     */
	    public static Date getFirstMonth(int monty) {
	        Calendar c = Calendar.getInstance();
	        c.add(Calendar.MONTH, -monty);
	        return c.getTime();
	    }
	    
	    /**
	     * 通过秒（如：1234561233）转换成yyyy-MM-dd HH:mm:ss
	     * @param sec
	     * @return
	     */
	    public static String getDateStrFromSecStr(String sec) {
	    	try {
	    		if (StringUtils.isNotBlank(sec)) {
	    			Date d = new Date(Long.valueOf(sec) * 1000);
	    			return dateTimeFormat.format(d);
	    		}
	    	} catch (Exception e) {
	    		e.printStackTrace();
	    	}
	    	return "";
	    }
	    
	    /**
	     * 获取时间格式：yyyy-MM-dd HH:mm:ss字符串时间的毫秒数
	     * @param date
	     * @param def
	     * @return
	     */
	    public static long getLongFromDateStr(String date,String def) {
	    	long rs = 0L;
	    	try {
	    		if (StringUtils.isNotBlank(date)) {
	    			rs =  dateTimeFormat.parse(date).getTime();
	    		} else {
	    			rs =  dateTimeFormat.parse(def).getTime();
	    		}
	    	} catch (Exception e) {
	    		e.printStackTrace();
	    	}
	    	return rs;
	    }
	    
	    /**
	     * 获取时间格式：yyyy-MM-dd HH:mm:ss字符串时间的毫秒数
	     * @param date
	     * @param def
	     * @return
	     */
	    public static long getLongFromDateStr(String date) {
	    	long rs = 0L;
	    	try {
	    		if (StringUtils.isNotBlank(date)) {
	    			rs =  dateTimeFormat.parse(date).getTime();
	    		}
	    	} catch (Exception e) {
	    		e.printStackTrace();
	    	}
	    	return rs;
	    }
	    
	    /**
	     * 通过毫秒（如：1234561233999）转换成yyyy-MM-dd HH:mm:ss
	     * @param sec
	     * @return
	     */
	    public static String getDateStrFromMsec(String sec) {
	    	try {
	    		if (StringUtils.isNotBlank(sec)) {
	    			Date d = new Date(Long.valueOf(sec));
	    			return dateTimeFormat.format(d);
	    		}
	    	} catch (Exception e) {
	    		e.printStackTrace();
	    	}
	    	return "";
	    }
	    
	    public static void main(String[] args) {
			System.out.println(0L+"");
			System.out.println(getDayBeforeOrAfter(null,0));
		}
	    
}
   

