package com.sklcc.express.DbObjs;

import java.sql.SQLException;

public interface DbQueryEnd {
	public DbResultSet first()throws SQLException;
	public DbResultSet get()throws SQLException;
	public DbResultSet max(String col)throws SQLException;
	public DbResultSet min(String col)throws SQLException;
	public DbResultSet avg(String col)throws SQLException;
	public int count()throws SQLException;
	public DbResultSet plunk(String col)throws SQLException;
	public DbObj value(String col)throws SQLException;
	public int update(DbColMap map)throws SQLException;
	public boolean insert(DbColMap map)throws SQLException;
	public DbResultSet increment(String col)throws SQLException;
	public DbResultSet decrement(String col)throws SQLException;
	public boolean delete()throws SQLException;
}
