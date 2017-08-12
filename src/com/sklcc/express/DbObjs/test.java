package com.sklcc.express.DbObjs;

import java.sql.SQLException;

public class test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DbConnection con=new DbConnection();
		con.connect("com.microsoft.sqlserver.jdbc.SQLServerDriver",
				"jdbc:sqlserver://10.10.64.196:1433;DatabaseName=doorpass_design_1;",
				"javauser", "12345678");
		DbResultSet rs=null;
		try {
			rs=DbQuery.in(con).table("dp_door_permissions").
					leftJoin("dp_doors", "dp_doors.id", "=", "dp_door_permissions.pms_door_id").
					leftJoin("dp_users", "dp_users.id", "=", "dp_door_permissions.pms_user_id").
					where("user_name","凌云").
					whereDate("door_create_time" ,">=","2017-08-06 17:16:08.0").
					get();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(rs);
		con.close();
	}

}
