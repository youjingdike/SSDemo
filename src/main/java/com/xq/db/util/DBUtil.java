package com.xq.db.util;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.UUID;

/**
 * 可优化：实现连接池的优化
 * @author xingqian
 *
 */
public class DBUtil {
	
	/*private static Properties properties = null;
	
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
    }*/
	
    /*public static Properties getDBInfo(){
//      InputStream is=DBUtil.class.getClassLoader().getResourceAsStream("db.properties");
        InputStream is=Thread.currentThread().getContextClassLoader().getResourceAsStream("db.properties");
        
        //如果位于包下面的资源。则可以
//      InputStream is=Thread.currentThread().getContextClassLoader().getResourceAsStream("com/bjsxt/jdbc/db.properties");
        properties = new Properties();
        try {
            properties.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }*/
    
	/*public static Connection getPhoenixConnect(){
		Connection conn=null;
		
		if(properties == null){
            getDBInfo();
        }
		
		try {
			// 下面的驱动为Phoenix老版本使用2.11使用，对应hbase0.94+
			// Class.forName("com.salesforce.phoenix.jdbc.PhoenixDriver");
			// phoenix4.3用下面的驱动对应hbase0.98+
			Class.forName(properties.getProperty("ph_driverName"));
			// 这里配置zookeeper的地址，可单个，也可多个。可以是域名或者ip
			String url = properties.getProperty("ph_url");
			conn = DriverManager.getConnection(url);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}*/
	
	/*public static Connection getPgConnect(){
		Connection conn=null;
		
		if(properties == null){
            getDBInfo();
        }
		
		try {
			String url=properties.getProperty("pg_url");
            String user=properties.getProperty("pg_userName");
            String password = properties.getProperty("pg_pass");
            Class.forName(properties.getProperty("pg_driverName"));
            conn= DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}*/
	
	public static void closeConnect(Connection conn) {
		ConnectPool.returnConn(conn);
	}
	
	/**
	 * 获取32位的UUID
	 * @return
	 */
	public static String getUUID() {
		String id = UUID.randomUUID().toString().replaceAll("-", "");
		return id;
	}
	
	public static void main(String []args) {
        /*Connection connection=null;
        try{
            String url="jdbc:postgresql://10.99.0.9:5432/logdb";
            String user="postgres";
            String password = "postgres";
            Class.forName("org.postgresql.Driver");
            connection= DriverManager.getConnection(url, user, password);
            System.out.println("是否成功连接pg数据库"+connection);
        }catch(Exception e){
            throw new RuntimeException(e);
        }finally{
            try{
                connection.close();
            }
            catch(SQLException e){
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        Connection phoenixConnect = getPhoenixConnect();
        System.out.println("连接到Phoenix："+phoenixConnect);
        closeConnect(phoenixConnect);*/
		/*Connection pgConnect = getPgConnect();
		if (pgConnect!=null) {
			System.out.println("pg已连接...");
		}
		
		Connection phoenixConnect = getPhoenixConnect();
		if (phoenixConnect!=null) {
			System.out.println("phoenix已连接...");
		}*/
    }
}
