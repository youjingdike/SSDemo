package com.xq.db.dao.inter;

import java.sql.ResultSet;

public interface ResultSetHandler<T> {
	T resultCallBack(ResultSet result) throws Exception;
}
