package com.sklcc.express.DbObjs;

import java.util.ArrayList;


public class DbRecord {
	public ArrayList<DbObj> data=new ArrayList<DbObj>();
	public DbResultSet rs=null;
	
	public <T> DbRecord add(int col,T val){
		data.add(new DbObj(val));
		return this;
	}
	public DbObj get(int col){
		return data.get(col);
	}
	public DbObj get(String colName){
		int i=0;
		for(;i<rs.colnames.size();++i){
			if(colName.equals(rs.colnames.get(i))){
				break;
			}
		}
		return data.get(i);
	}
	public String toString(){
		String rst="";
		for(DbObj each:data){
			rst+=each.getString()+"\t";
		}
		return rst;
	}
	/*
	public static DbRecord make(){
		DbRecord rst=new DbRecord();
		rst.data=new TreeMap<String,String>();
		return rst;
	}
	public DbRecord add(String f,String val){
		this.data.put(f, " '"+val+"' ");
		return this;
	}
	public DbRecord add(String f,int val){
		this.data.put(f, Integer.toString(val));
		return this;
	}
	*/
}
