package com.sklcc.express.DbObjs;

import com.sklcc.express.DbObjs.DbConnection.DbType;

public class DbDateNow {
	public static DbDateNow create(){
		return new DbDateNow();
	}
	public String getNowFunction(DbType type){
		if(type==DbType.SQLSERVER){
			return "getdate()";
		}else{
			return null;
		}
	}
}
