package com.sklcc.express.DbObjs;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

//import java.util.List;

public class DbObj {
	public String value=null;
	private DateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:m:s");
	public DbObj(int val){
		value=Integer.toString(val);
	}
	public DbObj(String val){
		value=val;
	}
	public DbObj(Date val){
		value=format.format(val);
	}
	public <T> DbObj(T val){
		if(val!=null)
			value=val.toString();
		else
			value=null;
	}
	
	public boolean isNull(){
		return value==null;
	}
	public String toString(){
		return value;
	}
	public String getString(){
		return value;
	}
	public int getInt(){
		return Integer.parseInt(value);
	}
	public Date getDate(){
		Date rst = null;
		try {
			rst=format.parse(value);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rst;
	}
}
