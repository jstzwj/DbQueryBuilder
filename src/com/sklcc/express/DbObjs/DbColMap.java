package com.sklcc.express.DbObjs;

import java.util.ArrayList;
//import java.util.TreeMap;

public class DbColMap {
	public ArrayList<DbPair<String,DbObj>> data;
	public int size(){
		return data.size();
	}
	public DbObj get(String col){
		DbObj rst = null;
		for(DbPair<String,DbObj> each:data){
			if(each.first.equals(col)){
				rst=each.second;
				break;
			}
		}
		return rst;
	}
	public DbPair<String,DbObj> get(int col){
		return data.get(col);
	}
	public static DbColMap make(){
		DbColMap rst=new DbColMap();
		rst.data=new ArrayList<DbPair<String,DbObj>>();
		return rst;
	}
	public DbColMap add(String f,String val){
		this.data.add(new DbPair<String, DbObj>(f,new DbObj(" '"+val+"' ")));
		return this;
	}
	public DbColMap addString(String f,String val){
		this.data.add(new DbPair<String, DbObj>(f,new DbObj(" '"+val+"' ")));
		return this;
	}
	public DbColMap add(String f,int val){
		this.data.add(new DbPair<String, DbObj>(f,new DbObj(Integer.toString(val))));
		return this;
	}
	public DbColMap addInt(String f,int val){
		this.data.add(new DbPair<String, DbObj>(f,new DbObj(Integer.toString(val))));
		return this;
	}
	public DbColMap addDate(String f,String val){
		this.data.add(new DbPair<String, DbObj>(f,new DbObj(val)));
		return this;
	}
}
