package com.xq.db.util;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.xq.db.dao.inter.ResultSetHandler;

/**
 * @author Robert HG (254963746@qq.com) on 3/9/16.
 */
public class RshHandler {

    public static final ResultSetHandler<List<Map<String,Object>>> toLIST = rs -> {
    	List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        ResultSetMetaData rsMetaData = rs.getMetaData();
        while (rs.next()) {
        	Map<String,Object> po = new HashMap<String,Object>();
        	int colCount = rsMetaData.getColumnCount();
            for (int i = 1; i <= colCount; i++) {
				String name = rsMetaData.getColumnName(i);
				po.put(name.toLowerCase(), rs.getObject(name));
			}
            list.add(po);
        }
        return list;
    };
    
    public static final ResultSetHandler<Map<String,Object>> toMAP = rs -> {
    	ResultSetMetaData rsMetaData = rs.getMetaData();
        Map<String,Object> po = new HashMap<String,Object>();
        while (rs.next()) {
        	int colCount = rsMetaData.getColumnCount();
            for (int i = 1; i <= colCount; i++) {
				String name = rsMetaData.getColumnName(i);
				po.put(name.toLowerCase(), rs.getObject(name));
			}
            return po;
        }
        return po;
    };
    
    public static final ResultSetHandler<Integer> getCot = rs -> {
        if (rs.next()) {
			return  rs.getInt(1);
        }
        return 0;
    };

}
