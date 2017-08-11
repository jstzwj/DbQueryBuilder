package com.sklcc.express.DbObjs;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;



public class DbQuerySelect extends DbQueryWhere implements DbQueryEnd{
	public ArrayList<String> selectCols=new ArrayList<String>();
	
	public String getColPart(){
		String rst="";
		for(int i=0;i<selectCols.size();++i){
			if(i!=0){
				rst+=",";
			}
			rst+=selectCols.get(i);
		}
		return rst;
	}
	
	public int update(DbColMap map) throws SQLException{
		//如果没有连接数据库，记录错误
		if (connection.dbConnection == null) {
			System.out.println("haven't inited the db!");
		}
		String sql = null;
		Statement stmt = null;
		int result = 0;
		try {
			//创建声明
			stmt = connection.dbConnection.createStatement();
			sql = "update "+table+" set ";
			for(int i=0;i<map.size();++i){
				if(i!=0){
					sql+=" , ";
				}
				sql+=map.get(i).first+" = "+map.get(i).second;
			}
			sql+=getWherePart();
			result = stmt.executeUpdate(sql);
			stmt.close();
		} catch (Exception e) {
			System.out.println("raw sql:"+sql);
			connection.dbConnection = null;
			throw e;
		}
		return result;
	}
	public DbResultSet first(){
		//如果没有连接数据库，记录错误
		if (connection.dbConnection == null) {
			System.out.println("haven't inited the db!");
		}
		String sql = null;
		Statement stmt = null;
		ResultSet rs=null;
		ResultSetMetaData meta_data=null;
		DbResultSet result=new DbResultSet();
		try {
			//创建声明
			stmt = connection.dbConnection.createStatement();
			sql = "SELECT "+getColPart()+" FROM "+table+" ";
			sql+=getWherePart();
			rs = stmt.executeQuery(sql);
			meta_data=rs.getMetaData();
			//添加列名
			int count=meta_data.getColumnCount();
			for(int i=1;i<=count;++i){
				result.colnames.add(meta_data.getColumnLabel(i));
			}
			if(rs.next()){
				DbRecord record=new DbRecord();
				for(int i=0;i<count;++i){
					record.add(i, rs.getString(i+1));
				}
				result.addRecord(record);
			}
			stmt.close();
			rs.close();
		} catch (Exception e) {
			System.out.println("raw sql:"+sql);
			connection.dbConnection = null;
		}
		return result;
	}
	public DbQuerySelect addSelect(String... args){
		
		for(int i=0;i<args.length;++i){
			this.selectCols.add(args[i]);
		}
		return this;
	}
	public DbResultSet get() throws SQLException{
		//如果没有连接数据库，记录错误
		if (connection.dbConnection == null) {
			System.out.println("haven't inited the db!");
		}
		String sql = null;
		Statement stmt = null;
		ResultSet rs=null;
		ResultSetMetaData meta_data=null;
		DbResultSet result=new DbResultSet();
		try {
			//创建声明
			stmt = connection.dbConnection.createStatement();
			sql = "SELECT "+getColPart()+" FROM "+table+" ";
			sql+=getWherePart();
			rs = stmt.executeQuery(sql);
			meta_data=rs.getMetaData();
			//添加列名
			int count=meta_data.getColumnCount();
			for(int i=1;i<=count;++i){
				result.colnames.add(meta_data.getColumnLabel(i));
			}
			while(rs.next()){
				DbRecord record=new DbRecord();
				for(int i=0;i<count;++i){
					record.add(i, rs.getString(i+1));
				}
				result.addRecord(record);
			}
			stmt.close();
			rs.close();
		} catch (Exception e) {
			System.out.println("raw sql:"+sql);
			connection.dbConnection = null;
			throw e;
		}
		return result;
	}
	@Override
	public DbResultSet max(String col) throws SQLException {
		//如果没有连接数据库，记录错误
		if (connection.dbConnection == null) {
			System.out.println("haven't inited the db!");
		}
		String sql = null;
		Statement stmt = null;
		ResultSet rs=null;
		ResultSetMetaData meta_data=null;
		DbResultSet result=new DbResultSet();
		try {
			//创建声明
			stmt = connection.dbConnection.createStatement();
			sql = "SELECT MAX("+col+") FROM "+table+" ";
			sql+=getWherePart();
			rs = stmt.executeQuery(sql);
			meta_data=rs.getMetaData();
			//添加列名
			int count=meta_data.getColumnCount();
			for(int i=1;i<=count;++i){
				result.colnames.add(meta_data.getColumnLabel(i));
			}
			//添加数据
			while(rs.next()){
				DbRecord record=new DbRecord();
				for(int i=0;i<count;++i){
					record.add(i, rs.getString(i+1));
				}
				result.addRecord(record);
			}
			stmt.close();
			rs.close();
		} catch (Exception e) {
			System.out.println("raw sql:"+sql);
			connection.dbConnection = null;
			throw e;
		}
		return result;
	}
	@Override
	public DbResultSet min(String col) throws SQLException {
		//如果没有连接数据库，记录错误
		if (connection.dbConnection == null) {
			System.out.println("haven't inited the db!");
		}
		String sql = null;
		Statement stmt = null;
		ResultSet rs=null;
		ResultSetMetaData meta_data=null;
		DbResultSet result=new DbResultSet();
		try {
			//创建声明
			stmt = connection.dbConnection.createStatement();
			sql = "SELECT MIN("+col+") FROM "+table+" ";
			sql+=getWherePart();
			rs = stmt.executeQuery(sql);
			meta_data=rs.getMetaData();
			//添加列名
			int count=meta_data.getColumnCount();
			for(int i=1;i<=count;++i){
				result.colnames.add(meta_data.getColumnLabel(i));
			}
			//添加数据
			while(rs.next()){
				DbRecord record=new DbRecord();
				for(int i=0;i<count;++i){
					record.add(i, rs.getString(i+1));
				}
				result.addRecord(record);
			}
			stmt.close();
			rs.close();
		} catch (Exception e) {
			System.out.println("raw sql:"+sql);
			connection.dbConnection = null;
			throw e;
		}
		return result;
	}
	@Override
	public DbResultSet avg(String col) throws SQLException {
		if (connection.dbConnection == null) {
			System.out.println("haven't inited the db!");
		}
		String sql = null;
		Statement stmt = null;
		ResultSet rs=null;
		ResultSetMetaData meta_data=null;
		DbResultSet result=new DbResultSet();
		try {
			//创建声明
			stmt = connection.dbConnection.createStatement();
			sql = "SELECT AVG("+col+") FROM "+table+" ";
			sql+=getWherePart();
			rs = stmt.executeQuery(sql);
			meta_data=rs.getMetaData();
			//添加列名
			int count=meta_data.getColumnCount();
			for(int i=1;i<=count;++i){
				result.colnames.add(meta_data.getColumnLabel(i));
			}
			//添加数据
			while(rs.next()){
				DbRecord record=new DbRecord();
				for(int i=0;i<count;++i){
					record.add(i, rs.getString(i+1));
				}
				result.addRecord(record);
			}
			stmt.close();
			rs.close();
		} catch (Exception e) {
			System.out.println("raw sql:"+sql);
			connection.dbConnection = null;
			throw e;
		}
		return result;
	}
	@Override
	public int count() throws SQLException {
		if (connection.dbConnection == null) {
			System.out.println("haven't inited the db!");
		}
		String sql = null;
		Statement stmt = null;
		ResultSet rs=null;
		int result=0;
		try {
			//创建声明
			stmt = connection.dbConnection.createStatement();
			sql = "SELECT COUNT(*) FROM "+table+" ";
			sql+=getWherePart();
			rs = stmt.executeQuery(sql);
			//添加数据
			if(rs.next()){
				result=rs.getInt(1);
			}
			stmt.close();
			rs.close();
		} catch (Exception e) {
			System.out.println("raw sql:"+sql);
			connection.dbConnection = null;
			throw e;
		}
		return result;
	}
	@Override
	public DbResultSet plunk(String str) throws SQLException {
		if (connection.dbConnection == null) {
			System.out.println("haven't inited the db!");
		}
		String sql = null;
		Statement stmt = null;
		ResultSet rs=null;
		ResultSetMetaData meta_data=null;
		DbResultSet result=new DbResultSet();
		try {
			//创建声明
			stmt = connection.dbConnection.createStatement();
			sql = "SELECT "+str+" FROM "+table+" ";
			sql+=getWherePart();
			rs = stmt.executeQuery(sql);
			meta_data=rs.getMetaData();
			//添加列名
			int count=meta_data.getColumnCount();
			for(int i=1;i<=count;++i){
				result.colnames.add(meta_data.getColumnLabel(i));
			}
			//添加数据
			while(rs.next()){
				DbRecord record=new DbRecord();
				for(int i=0;i<count;++i){
					record.add(i, rs.getString(i+1));
				}
				result.addRecord(record);
			}
			stmt.close();
			rs.close();
		} catch (Exception e) {
			System.out.println("raw sql:"+sql);
			connection.dbConnection = null;
			throw e;
		}
		return result;
	}
	@Override
	public DbObj value(String col) throws SQLException {
		//如果没有连接数据库，记录错误
		if (connection.dbConnection == null) {
			System.out.println("haven't inited the db!");
		}
		String sql = null;
		Statement stmt = null;
		ResultSet rs=null;
		DbObj result=null;
		try {
			//创建声明
			stmt = connection.dbConnection.createStatement();
			sql = "SELECT "+col+" FROM "+table+" ";
			sql+=getWherePart();
			rs = stmt.executeQuery(sql);
			if(rs.next()){
				result=new DbObj(rs.getString(1));
			}
			stmt.close();
			rs.close();
		} catch (Exception e) {
			System.out.println("raw sql:"+sql);
			connection.dbConnection = null;
		}
		return result;
	}
	@Override
	public DbResultSet increment(String col) throws SQLException {
		//如果没有连接数据库，记录错误
		if (connection.dbConnection == null) {
			System.out.println("haven't inited the db!");
		}
		String sql = null;
		Statement stmt = null;
		ResultSet rs=null;
		ResultSetMetaData meta_data=null;
		DbResultSet result=new DbResultSet();
		try {
			//创建声明
			stmt = connection.dbConnection.createStatement();
			sql = "SELECT "+getColPart()+" FROM "+table+" ";
			sql+=getWherePart();
			sql+=" ORDER BY "+col+" ASC ";
			rs = stmt.executeQuery(sql);
			meta_data=rs.getMetaData();
			//添加列名
			int count=meta_data.getColumnCount();
			for(int i=1;i<=count;++i){
				result.colnames.add(meta_data.getColumnLabel(i));
			}
			while(rs.next()){
				DbRecord record=new DbRecord();
				for(int i=0;i<count;++i){
					record.add(i, rs.getString(i+1));
				}
				result.addRecord(record);
			}
			stmt.close();
			rs.close();
		} catch (Exception e) {
			System.out.println("raw sql:"+sql);
			connection.dbConnection = null;
			throw e;
		}
		return result;
	}
	@Override
	public DbResultSet decrement(String col) throws SQLException {
		//如果没有连接数据库，记录错误
		if (connection.dbConnection == null) {
			System.out.println("haven't inited the db!");
		}
		String sql = null;
		Statement stmt = null;
		ResultSet rs=null;
		ResultSetMetaData meta_data=null;
		DbResultSet result=new DbResultSet();
		try {
			//创建声明
			stmt = connection.dbConnection.createStatement();
			sql = "SELECT "+getColPart()+" FROM "+table+" ";
			sql+=getWherePart();
			sql+=" ORDER BY "+col+" DESC ";
			rs = stmt.executeQuery(sql);
			meta_data=rs.getMetaData();
			//添加列名
			int count=meta_data.getColumnCount();
			for(int i=1;i<=count;++i){
				result.colnames.add(meta_data.getColumnLabel(i));
			}
			while(rs.next()){
				DbRecord record=new DbRecord();
				for(int i=0;i<count;++i){
					record.add(i, rs.getString(i+1));
				}
				result.addRecord(record);
			}
			stmt.close();
			rs.close();
		} catch (Exception e) {
			System.out.println("raw sql:"+sql);
			connection.dbConnection = null;
			throw e;
		}
		return result;
	}
	@Override
	public boolean delete() throws SQLException {
		if (connection.dbConnection == null) {
			System.out.println("haven't inited the db!");
		}
		String sql = null;
		Statement stmt = null;
		boolean result=false;
		try {
			//创建声明
			stmt = connection.dbConnection.createStatement();
			sql = "DELETE FROM "+table+" ";
			sql+=getWherePart();
			result = stmt.execute(sql);
			stmt.close();
		} catch (Exception e) {
			System.out.println("raw sql:"+sql);
			connection.dbConnection = null;
			throw e;
		}
		return result;
	}
}
