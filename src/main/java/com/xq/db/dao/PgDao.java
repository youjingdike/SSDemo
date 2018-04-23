package com.xq.db.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.xq.db.dao.inter.JDBCTemplateInter;
import com.xq.db.util.ConnectPool;


public class PgDao implements JDBCTemplateInter{
	
	/**
	 * 
	 * @param conn
	 * @param info
	 * @throws SQLException
	 */
	public void insertDB(String info) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			conn = getConn();
			boolean isAuto = conn.getAutoCommit();
			conn.setAutoCommit(false);
			String sql = "insert into test_xq(info) values(?)";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1,info);
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
		return ConnectPool.getPgConnection();
	}
	
	public static void main(String[] args) {
		new PgDao().insertDB("ddddd");
	}

}
