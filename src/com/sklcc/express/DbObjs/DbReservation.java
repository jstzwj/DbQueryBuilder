package com.sklcc.express.DbObjs;

import java.util.Date;

public class DbReservation {
	public Integer id=null;
	public Date rsv_time_start=null;
	public Date rsv_time_end=null;
	public Integer rsv_door_id=null;
	public Integer rsv_reserve_door_user_id=null;
	public Integer rsv_open_door_user_id=null;
	public Integer rsv_need_record_late=null;
	public Date rsv_open_door_time=null;
	public Date rsv_origin_time_end=null;
}
