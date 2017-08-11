package com.sklcc.express.DbObjs;

import java.util.ArrayList;
//import java.util.TreeMap;

public class DbResultSet {
	public ArrayList<DbRecord> records=new ArrayList<DbRecord>();
	public ArrayList<String> colnames=new ArrayList<String>();
	
	public void addRecord(DbRecord record){
		record.rs=this;
		records.add(record);
	}
	public DbRecord getRecord(int n){
		if(n>=0&&n<records.size()){
			return records.get(n);
		}
		else{
			return null;
		}
		
	}
	public int size(){
		return records.size();
	}
	
	public String toString(){
		String rst="";
		for(String each:colnames){
			rst+=each+"\t";
		}
		rst+="\n";
		for(DbRecord each:records){
			rst+=each+"\n";
		}
		return rst;
	}
	
}
