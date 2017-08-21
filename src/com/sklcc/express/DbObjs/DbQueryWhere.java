package com.sklcc.express.DbObjs;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;



public class DbQueryWhere extends DbQueryTable implements DbQueryEnd{
	
	ArrayList<String> whereCond=new ArrayList<String>();
	public String getWherePart(){
		String rst="";
		rst+=" WHERE ";
		for(int i=0;i<whereCond.size();++i){
			if(i!=0){
				rst+=" AND ";
			}
			rst+=whereCond.get(i);
		}
		return rst;
	}
	public DbQueryWhere where(String col,int val){
		String cond;
		cond=col.toString()+" = "+Integer.toString(val);
		this.whereCond.add(cond);
		return this;
	}
	public DbQueryWhere where(String col,String r,int val){
		String cond;
		cond=col.toString()+r+Integer.toString(val);
		this.whereCond.add(cond);
		return this;
	}
	public DbQueryWhere where(String col,String val){
		String cond;
		if(val==null)
			cond=col.toString()+" is null";
		else
			cond=col.toString()+"="+" '"+val.toString()+"' ";
		this.whereCond.add(cond);
		return this;
	}
	public DbQueryWhere where(String col,String r,String val){
		String cond;
		if(val==null)
			cond=col.toString()+" is null";
		else
			cond=col.toString()+"="+" '"+val.toString()+"' ";
		this.whereCond.add(cond);
		return this;
	}
	public DbQueryWhere whereBetween(String col,int start,int end){
		String cond;
		cond=" "+col+" BETWEEN "+Integer.toString(start)+" AND "+Integer.toString(end)+" ";
		this.whereCond.add(cond);
		return this;
	}
	public DbQueryWhere whereBetween(String col,String start,String end){
		String cond;
		cond=" "+col+" BETWEEN \'"+start+"\' AND \'"+end+"\' ";
		this.whereCond.add(cond);
		return this;
	}
	public DbQueryWhere whereNotBetween(String col,int start,int end){
		String cond;
		cond=" "+col+" NOT BETWEEN "+Integer.toString(start)+" AND "+Integer.toString(end)+" ";
		this.whereCond.add(cond);
		return this;
	}
	public DbQueryWhere whereNotBetween(String col,String start,String end){
		String cond;
		cond=" "+col+" NOT BETWEEN \'"+start+"\' AND \'"+end+"\' ";
		this.whereCond.add(cond);
		return this;
	}
	public DbQueryWhere whereNull(String col){
		String cond;
		cond=" "+col+" IS NULL ";
		this.whereCond.add(cond);
		return this;
	}
	public DbQueryWhere whereNotNull(String col){
		String cond;
		cond=" "+col+" IS NOT NULL ";
		this.whereCond.add(cond);
		return this;
	}
	public DbQueryWhere whereDate(String col, Date date){
		String cond;
		cond=" "+col+" = cast('"+df.format(date)+"' as datetime)";
		this.whereCond.add(cond);
		return this;
	}
	public DbQueryWhere whereDate(String col, String date){
		String cond;
		cond=" "+col+" = cast('"+date+"' as datetime)";
		this.whereCond.add(cond);
		return this;
	}
	public DbQueryWhere whereDate(String col, DbDateNow date){
		String cond;
		cond=" "+col+" = "+date.getNowFunction(connection.dbType)+" ";
		this.whereCond.add(cond);
		return this;
	}
	public DbQueryWhere whereDate(String col,String cmp, Date date){
		String cond;
		cond=" "+col+" "+cmp+" cast('"+df.format(date)+"' as datetime)";
		this.whereCond.add(cond);
		return this;
	}
	public DbQueryWhere whereDate(String col,String cmp, String date){
		String cond;
		cond=" "+col+" "+cmp+" cast('"+date+"' as datetime)";
		this.whereCond.add(cond);
		return this;
	}
	public DbQueryWhere whereDate(String col,String cmp, DbDateNow date){
		String cond;
		cond=" "+col+" "+cmp+" "+date.getNowFunction(connection.dbType)+" ";
		this.whereCond.add(cond);
		return this;
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
				sql+=map.get(i).first+" = "+map.get(i).second.getString();
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
			sql = "SELECT * FROM "+table+" ";
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
	public DbQuerySelect select(String... args){
		DbQuerySelect rst=new DbQuerySelect();
		rst.table=this.table;
		rst.whereCond=this.whereCond;
		
		for(int i=0;i<args.length;++i){
			rst.selectCols.add(args[i]);
		}
		return rst;
		/*
			String cols="";
			for(int i=0;i<args.length;++i){
				if(i!=0){
					cols+=",";
				}
				cols+=args[i];
			}
		*/
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
			sql = "SELECT * FROM "+table+" ";
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
	public DbResultSet plunk(String col) throws SQLException {
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
			sql = "SELECT "+col+" FROM "+table+" ";
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
			sql = "SELECT * FROM "+table+" ";
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
			sql = "SELECT * FROM "+table+" ";
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
