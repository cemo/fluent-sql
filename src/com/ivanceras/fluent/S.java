package com.ivanceras.fluent;

/**
 * S stands for static SQL, mostly just short cuts
 * @author lee
 *
 */
public class S {
	
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
	public static SQL COUNT(String column){
		return new SQL().COUNT(column);
	}
	public static SQL MIN(String column){
		return new SQL().MIN(column);
	}
	public static SQL MAX(String column){
		return new SQL().MAX(column);
	}
	public static SQL SUM(String column){
		return new SQL().SUM(column);
	}
	public static SQL AVG(String column){
		return new SQL().AVG(column);
	}

}
