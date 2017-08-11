package com.sklcc.express.DbObjs;

import java.sql.Connection;
import java.sql.DriverManager;

public class DbConnection {
	public Connection dbConnection = null;
	
	public DbConnection() {
		
	}
	public void connect(String driver,String url,String user,String password){
		//如果没连接，接数据库
		if (dbConnection == null) {
			try {
				//连接
				Class.forName(driver);
				dbConnection = DriverManager.getConnection(url, user, password);
			} catch (Exception e) {
				e.printStackTrace();
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
			}
		}
	}
	public void close(){
		if (dbConnection != null) {
			try {
				dbConnection.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
