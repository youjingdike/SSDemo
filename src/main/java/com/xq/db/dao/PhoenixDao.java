package com.xq.db.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.xq.db.dao.inter.JDBCTemplateInter;
import com.xq.db.util.ConnectPool;
import com.xq.db.util.DBUtil;
import com.xq.db.util.RshHandler;


public class PhoenixDao implements JDBCTemplateInter{
	
	/**
	 * 
	 * @param conn
	 * @param info
	 * @throws SQLException
	 */
	public void insertDB(String id,String info) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			conn = getConn();
			boolean isAuto = conn.getAutoCommit();
			conn.setAutoCommit(false);
			String sql = "upsert into test1(id,text) values(?,?)";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1,id);
			pstmt.setString(2,info);
			pstmt.addBatch();
			pstmt.executeBatch();
			conn.commit();
			conn.setAutoCommit(isAuto);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (pstmt!=null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (conn!=null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	@Override
	public Connection getConn() throws Exception {
		return ConnectPool.getPhConnection();
	}
	
	public static void main(String[] args) {
		PhoenixDao phoenixDao = new PhoenixDao();
		phoenixDao.insertDB(DBUtil.getUUID(), "testssss");
		List<Map<String,Object>> list = phoenixDao.doQuery("select * from test", null, RshHandler.toLIST);
		list.forEach(s->{
			s.forEach((k,v)->{
				System.out.println(k+v);
			});
		});
	}

}
