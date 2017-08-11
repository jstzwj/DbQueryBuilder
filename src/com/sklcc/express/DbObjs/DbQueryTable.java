package com.sklcc.express.DbObjs;

import java.sql.SQLException;
import java.sql.Statement;
//import java.util.ArrayList;



public class DbQueryTable extends DbQuery {
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
	
}
