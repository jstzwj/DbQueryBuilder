package com.sklcc.express.DbObjs;

public class DbPair<T,E>{
	public DbPair(T first,E second){
		this.first=first;
		this.second=second;
	}
	public T first;
	public E second;
}