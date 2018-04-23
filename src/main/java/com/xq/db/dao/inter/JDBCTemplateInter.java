package com.xq.db.dao.inter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

public interface JDBCTemplateInter {
	
	Connection getConn() throws Exception;
	
	/**
	 * 执行查询
	 * @param conn
	 * @param sql
	 * @param paramsList
	 * @param callBack
	 * @return
	 */
	default <T> T doQuery(String sql, Object[] paramsList, ResultSetHandler<T> callBack) {
System.out.println(sql);
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		T result = null;
		try {
			conn = getConn();
			preparedStatement = conn.prepareStatement(sql);

			if(paramsList!=null){
				for (int i = 0; i < paramsList.length; i++) {
					preparedStatement.setObject(i + 1, paramsList[i]);
				}
			}

			rs = preparedStatement.executeQuery();

			result = callBack.resultCallBack(rs);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs!=null) {
					rs.close();
				}
				if (preparedStatement != null) {
					preparedStatement.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	
	/**
	 * 批量执行,没有返回结果
	 * @param conn
	 * @param sqlText
	 * @param paramsList
	 */
	default void doBatchVoid(String sqlText, List<Object[]> paramsList) {
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		int count = 0;
		try {
			conn = getConn();
			boolean isAuto = conn.getAutoCommit();
			conn.setAutoCommit(false);
			preparedStatement = conn.prepareStatement(sqlText);
			
			for (Object[] parameters : paramsList) {
				count++;
				for (int i = 0; i < parameters.length; i++) {
					preparedStatement.setObject(i + 1, parameters[i]);
				}
				
				preparedStatement.addBatch();
				
				if (count % 500 == 0) {
					preparedStatement.executeBatch();
					conn.commit();
				}
			}
			
			preparedStatement.executeBatch();
			
			conn.commit();
			
			conn.setAutoCommit(isAuto);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
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
	
	/**
	 * 批量执行,有返回结果
	 * @param sqlText
	 * @param paramsList
	 * @return
	 */
	default int[] doBatch(String sqlText, List<Object[]> paramsList) {
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		int[] result = null;
		try {
			conn = getConn();
			boolean isAuto = conn.getAutoCommit();
			conn.setAutoCommit(false);
			preparedStatement = conn.prepareStatement(sqlText);

			for (Object[] parameters : paramsList) {
				for (int i = 0; i < parameters.length; i++) {
					preparedStatement.setObject(i + 1, parameters[i]);
				}

				preparedStatement.addBatch();
			}

			result = preparedStatement.executeBatch();

			conn.commit();
			conn.setAutoCommit(isAuto);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
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

		return result;
	}
	
	/**
	 * 获取32位的UUID
	 * @return
	 */
	public static String getUUID() {
		String id = UUID.randomUUID().toString().replaceAll("-", "");
		return id;
	}
	
}
