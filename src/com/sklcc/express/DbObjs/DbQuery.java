package com.sklcc.express.DbObjs;

//import java.sql.ResultSet;
//import java.sql.ResultSetMetaData;
//import java.sql.Statement;
//import java.util.ArrayList;
//import java.util.List;

//import com.sklcc.express.Db;
//import com.sklcc.express.Log;


public class DbQuery {
	DbConnection connection=null;
	public static DbQuery in(DbConnection con){
		DbQuery rst=new DbQuery();
		rst.connection=con;
		return rst;
	}
	public DbQueryTable table(String tb){
		DbQueryTable obj=new DbQueryTable();
		obj.connection=connection;
		obj.table=tb;
		return obj;
	}
}
