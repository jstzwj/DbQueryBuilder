package com.sklcc.express.DbObjs;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
//import java.util.ArrayList;
import java.util.Date;



public class DbQueryTable extends DbQuery implements DbQueryEnd {
	protected static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	String table="";
	public DbQueryWhere where(String col,int val){
		DbQueryWhere obj=new DbQueryWhere();
		obj.connection=this.connection;
		obj.table=this.table;
		String cond;
		cond=col.toString()+" = "+Integer.toString(val);
		obj.whereCond.add(cond);
		return obj;
	}
	public DbQueryWhere where(String col,int val,String r){
		DbQueryWhere obj=new DbQueryWhere();
		obj.connection=this.connection;
		obj.table=this.table;
		String cond;
		cond=col.toString()+r+Integer.toString(val);
		obj.whereCond.add(cond);
		return obj;
	}
	public DbQueryWhere where(String col,String val){
		DbQueryWhere obj=new DbQueryWhere();
		obj.connection=this.connection;
		obj.table=this.table;
		String cond;
		if(val==null)
			cond=col.toString()+" is null";
		else
			cond=col.toString()+"="+" '"+val.toString()+"' ";
		obj.whereCond.add(cond);
		return obj;
	}
	public DbQueryWhere where(String col,String val,String r){
		DbQueryWhere obj=new DbQueryWhere();
		obj.connection=this.connection;
		obj.table=this.table;
		String cond;
		if(val==null){
			if(r.equals("==")||r.equals("="))
				cond=col.toString()+" is null";
			else
				cond=col.toString()+" is not null";
		}
		else{
			cond=col.toString()+"="+" '"+val.toString()+"' ";
		}
		obj.whereCond.add(cond);
		return obj;
	}
	
	public DbQueryWhere whereBetween(String col,int start,int end){
		DbQueryWhere obj=new DbQueryWhere();
		obj.connection=this.connection;
		obj.table=this.table;
		String cond;
		cond=" "+col+" BETWEEN "+Integer.toString(start)+" AND "+Integer.toString(end)+" ";
		obj.whereCond.add(cond);
		return obj;
	}
	public DbQueryWhere whereBetween(String col,String start,String end){
		DbQueryWhere obj=new DbQueryWhere();
		obj.connection=this.connection;
		obj.table=this.table;
		String cond;
		cond=" "+col+" BETWEEN \'"+start+"\' AND \'"+end+"\' ";
		obj.whereCond.add(cond);
		return obj;
	}
	public DbQueryWhere whereNotBetween(String col,int start,int end){
		DbQueryWhere obj=new DbQueryWhere();
		obj.connection=this.connection;
		obj.table=this.table;
		String cond;
		cond=" "+col+" NOT BETWEEN "+Integer.toString(start)+" AND "+Integer.toString(end)+" ";
		obj.whereCond.add(cond);
		return obj;
	}
	public DbQueryWhere whereNotBetween(String col,String start,String end){
		DbQueryWhere obj=new DbQueryWhere();
		obj.connection=this.connection;
		obj.table=this.table;
		String cond;
		cond=" "+col+" NOT BETWEEN \'"+start+"\' AND \'"+end+"\' ";
		obj.whereCond.add(cond);
		return obj;
	}
	public DbQueryWhere whereNull(String col){
		DbQueryWhere obj=new DbQueryWhere();
		obj.connection=this.connection;
		obj.table=this.table;
		String cond;
		cond=" "+col+" IS NULL ";
		obj.whereCond.add(cond);
		return obj;
	}
	public DbQueryWhere whereNotNull(String col){
		DbQueryWhere obj=new DbQueryWhere();
		obj.connection=this.connection;
		obj.table=this.table;
		String cond;
		cond=" "+col+" IS NOT NULL ";
		obj.whereCond.add(cond);
		return obj;
	}
	
	public DbQueryTable join(String tbl,String left,String cond,String right){
		table+=" INNER JOIN "+tbl+" ON "+left+" "+cond+" "+right+" ";
		return this;
	}
	public DbQueryTable leftJoin(String tbl,String left,String cond,String right){
		table+=" LEFT JOIN "+tbl+" ON "+left+" "+cond+" "+right+" ";
		return this;
	}
	public DbQueryWhere whereDate(String col, Date date){
		DbQueryWhere obj=new DbQueryWhere();
		obj.connection=this.connection;
		obj.table=this.table;
		String cond;
		cond=" "+col+" = cast('"+df.format(date)+"' as datetime)";
		obj.whereCond.add(cond);
		return obj;
	}
	public DbQueryWhere whereDate(String col, String date){
		DbQueryWhere obj=new DbQueryWhere();
		obj.connection=this.connection;
		obj.table=this.table;
		String cond;
		cond=" "+col+" = cast('"+date+"' as datetime)";
		obj.whereCond.add(cond);
		return obj;
	}
	public DbQueryWhere whereDate(String col,String cmp, Date date){
		DbQueryWhere obj=new DbQueryWhere();
		obj.connection=this.connection;
		obj.table=this.table;
		String cond;
		cond=" "+col+" "+cmp+" cast('"+df.format(date)+"' as datetime)";
		obj.whereCond.add(cond);
		return obj;
	}
	public DbQueryWhere whereDate(String col,String cmp, String date){
		DbQueryWhere obj=new DbQueryWhere();
		obj.connection=this.connection;
		obj.table=this.table;
		String cond;
		cond=" "+col+" "+cmp+" cast('"+date+"' as datetime)";
		obj.whereCond.add(cond);
		return obj;
	}
	
	
	public boolean insert(DbColMap map) throws SQLException{
		//如果没有连接数据库，记录错误
		if (connection.dbConnection == null) {
			System.out.println("haven't inited the db!");
		}
		String sql = null;
		Statement stmt = null;
		boolean result = false;
		try {
			//创建声明
			stmt = connection.dbConnection.createStatement();
			sql = "insert into "+table+" ( ";
			for(int i=0;i<map.size();++i){
				if(i!=0){
					sql+=" , ";
				}
				sql+=map.get(i).first;
			}
			sql+=" ) values ( ";
			for(int i=0;i<map.size();++i){
				if(i!=0){
					sql+=" , ";
				}
				sql+=map.get(i).second.toString();
			}
			sql+=" ) ;";
			result = stmt.execute(sql);
			stmt.close();
		} catch (Exception e) {
			System.out.println("raw sql:"+sql);
			connection.dbConnection = null;
			throw e;
		}
		return result;
	}
	@Override
	public DbResultSet first() throws SQLException {
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
	@Override
	public DbResultSet get() throws SQLException {
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
	public int update(DbColMap map) throws SQLException {
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
			result = stmt.executeUpdate(sql);
			stmt.close();
		} catch (Exception e) {
			System.out.println("raw sql:"+sql);
			connection.dbConnection = null;
			throw e;
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
