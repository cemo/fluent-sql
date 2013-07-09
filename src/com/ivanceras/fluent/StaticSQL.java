package com.ivanceras.fluent;


/**
 * This is a streamline version of the Previous EntityManager
 * @author lee
 *
 */
public class StaticSQL{


	static StaticSQL singleton;

	public SQL SQL(){
		SQL sql = new SQL();
		singleton = this;
		return sql;
	}

	
	public static SQL WITH(String name, SQL sql){
		return new SQL().WITH(name, sql);
	}
	public static SQL WITH_RECURSIVE(String name, SQL sql){
		return new SQL().WITH_RECURSIVE(name, sql);
	}
	
	public static SQL SELECT(){
		return new SQL().SELECT();
	}
	public static SQL SELECT(String... column){
		return new SQL().SELECT(column);
	}
	
	public static SQL SELECT_ALL(){
		return new SQL().SELECT_ALL();
	}
	
	
	///////////////////////
	// One for SQL one for string column
	///////////////////////////
	
	public static SQL COUNT(String column){
		return new SQL().COUNT(column);
	}
	public static SQL COUNT(SQL sql){
		return new SQL().COUNT(sql);
	}
	
	/////////////////////
	// MIN
	/////////////////////
	public static SQL MIN(String column){
		return new SQL().MIN(column);
	}
	public static SQL MIN(SQL sql){
		return new SQL().MIN(sql);
	}
	
	/////////////////////
	// MAX
	/////////////////////
	public static SQL MAX(String column){
		return new SQL().MAX(column);
	}
	public static SQL MAX(SQL sql){
		return new SQL().MAX(sql);
	}
	
	/////////////////////
	// SUM
	/////////////////////
	public static SQL SUM(String column){
		return new SQL().SUM(column);
	}
	public static SQL SUM(SQL sql){
		return new SQL().SUM(sql);
	}
	
	/////////////////////
	// AVG
	/////////////////////
	public static SQL AVG(String column){
		return new SQL().AVG(column);
	}
	
	public static SQL AVG(SQL sql){
		return new SQL().AVG(sql);
	}
	
	/////////////////////
	// LOWER
	/////////////////////
	public static SQL LOWER(String column){
		return new SQL().LOWER(column);
	}
	
	public static SQL LOWER(SQL sql){
		return new SQL().LOWER(sql);
	}
	
	/////////////////////
	// UPPER
	/////////////////////
	public static SQL UPPER(String column){
		return new SQL().UPPER(column);
	}
	
	public static SQL UPPER(SQL sql){
		return new SQL().UPPER(sql);
	}
	
}
