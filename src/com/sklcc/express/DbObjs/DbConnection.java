package com.sklcc.express.DbObjs;

import java.sql.Connection;
import java.sql.DriverManager;


public class DbConnection {
	public Connection dbConnection = null;
	public enum DbType{UNKNOWN,SQLSERVER,MYSQL,ORACLE};
	public DbType dbType;
	
	public DbConnection() {
		dbType=DbType.UNKNOWN;
	}
	public DbConnection(DbType type) {
		dbType=type;
	}
	public void connect(String driver,String url,String user,String password) throws Exception{
		//如果没连接，接数据库
		if (dbConnection == null) {
			try {
				//连接
				Class.forName(driver);
				dbConnection = DriverManager.getConnection(url, user, password);
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			}
		}else{
			try {
				//暂存链接
				Connection tmp=dbConnection;
				//连接
				Class.forName(driver);
				dbConnection = DriverManager.getConnection(url, user, password);
				if(!tmp.isClosed()){
					tmp.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			}
		}
	}
	public void close() throws Exception{
		if (dbConnection != null) {
			try {
				dbConnection.close();
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			}
		}
	}
}
