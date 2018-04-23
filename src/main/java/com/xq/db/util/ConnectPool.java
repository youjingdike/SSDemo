package com.xq.db.util;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;

public class ConnectPool {
	private static Properties properties = null;
	/**pg数据源*/
	private static BasicDataSource bs_pg = null;
	/**phoenix数据源*/
	private static BasicDataSource bs_ph = null;
	
	static {
//      InputStream is=DBUtil.class.getClassLoader().getResourceAsStream("db.properties");
        InputStream is=Thread.currentThread().getContextClassLoader().getResourceAsStream("jdbc.properties");
        
        //如果位于包下面的资源。则可以
//      InputStream is=Thread.currentThread().getContextClassLoader().getResourceAsStream("com/bjsxt/jdbc/db.properties");
        properties = new Properties();
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }	

    /**
     * 创建pg数据源
     * @return
     */
    public static BasicDataSource getPgDataSource() throws Exception{
        if(bs_pg==null){
            bs_pg = new BasicDataSource();
            bs_pg.setDriverClassName(properties.getProperty("pg_driverName"));
            bs_pg.setUrl(properties.getProperty("pg_url"));
            bs_pg.setUsername(properties.getProperty("pg_userName"));
            bs_pg.setPassword(properties.getProperty("pg_pass"));
            
            bs_pg.setMaxActive(Integer.parseInt(properties.getProperty("pg_MaxActive")));//设置最大并发数
            bs_pg.setInitialSize(Integer.parseInt(properties.getProperty("pg_InitialSize")));//数据库初始化时，创建的连接个数
            bs_pg.setMinIdle(Integer.parseInt(properties.getProperty("pg_MinIdle")));//最小空闲连接数
            bs_pg.setMaxIdle(Integer.parseInt(properties.getProperty("pg_MaxIdle")));//数据库最大连接数
            bs_pg.setMaxWait(Long.parseLong(properties.getProperty("pg_MaxWait")));
            bs_pg.setMinEvictableIdleTimeMillis(Long.parseLong(properties.getProperty("pg_MinEvictableIdle")));//空闲连接60秒中后释放
            bs_pg.setTimeBetweenEvictionRunsMillis(Long.parseLong(properties.getProperty("pg_EvictionRunsMillis")));//5分钟检测一次是否有死掉的线程
            bs_pg.setTestOnBorrow(true);
            
            /*bs_pg.setMaxActive(200);//设置最大并发数
            bs_pg.setInitialSize(30);//数据库初始化时，创建的连接个数
            bs_pg.setMinIdle(50);//最小空闲连接数
            bs_pg.setMaxIdle(200);//数据库最大连接数
            bs_pg.setMaxWait(1000);
            bs_pg.setMinEvictableIdleTimeMillis(60*1000);//空闲连接60秒中后释放
            bs_pg.setTimeBetweenEvictionRunsMillis(5*60*1000);//5分钟检测一次是否有死掉的线程
            bs_pg.setTestOnBorrow(true);*/
        }
        return bs_pg;
    }

    /**
     * 释放pg数据源
     */
    public static void shutDownPgDataSource() throws Exception{
        if(bs_pg!=null){
            bs_pg.close();
        }
    }
    
    /**
     * 获取pg数据库连接
     * @return
     */
    public static Connection getPgConnection(){
        Connection con=null;
        try {
            if(bs_pg!=null){
                con=bs_pg.getConnection();
            }else{
                con=getPgDataSource().getConnection();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return con;
    }
    
    /**
     * 创建phoenix数据源
     * @return
     */
    public static BasicDataSource getPhDataSource() throws Exception{
        if(bs_ph==null){
            bs_ph = new BasicDataSource();
            bs_ph.setDriverClassName(properties.getProperty("ph_driverName"));
            bs_ph.setUrl(properties.getProperty("ph_url"));
            
            bs_ph.setMaxActive(Integer.parseInt(properties.getProperty("ph_MaxActive")));//设置最大并发数
            bs_ph.setInitialSize(Integer.parseInt(properties.getProperty("ph_InitialSize")));//数据库初始化时，创建的连接个数ph            
            bs_ph.setMinIdle(Integer.parseInt(properties.getProperty("ph_MinIdle")));//最小空闲连接数
            bs_ph.setMaxIdle(Integer.parseInt(properties.getProperty("ph_MaxIdle")));//数据库最大连接数
            bs_ph.setMaxWait(Long.parseLong(properties.getProperty("ph_MaxWait")));
            bs_ph.setMinEvictableIdleTimeMillis(Long.parseLong(properties.getProperty("ph_MinEvictableIdle")));//空闲连接60秒中后释放
            bs_ph.setTimeBetweenEvictionRunsMillis(Long.parseLong(properties.getProperty("ph_EvictionRunsMillis")));//5分钟检测一次是否有死掉的线程
            bs_ph.setTestOnBorrow(true);
        }
        return bs_ph;
    }

    /**
     * 释放phoenix数据源
     */
    public static void shutDownPhDataSource() throws Exception{
        if(bs_ph!=null){
            bs_ph.close();
        }
    }
    
    /**
     * 获取phoenix数据库连接
     * @return
     */
    public static Connection getPhConnection(){
        Connection con=null;
        try {
            if(bs_ph!=null){
                con=bs_ph.getConnection();
            }else{
                con=getPhDataSource().getConnection();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return con;
    }
    
    /**
     * 释放连接
     * 说明：使用完连接必须释放掉，否则会出现问题
     * @param conn
     */
    public static void returnConn(Connection conn) {
    	if (conn!=null) {
    		try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
    	}
    }
}
