package com.ivanceras.fluent;

import java.util.LinkedList;

public class Breakdown{

	private StringBuilder sql;
	private LinkedList<Object> parameters;
	boolean doComma = false;
	public Breakdown(String sql, LinkedList<Object> parameters){
		this.sql = new StringBuilder(sql.toString());
		this.parameters = parameters;
	}
	
	public Breakdown(){
		this.sql = new StringBuilder();
		this.parameters = new LinkedList<Object>();
	}
	
	public String getSql(){
		return sql.toString();
	}
	
	public void append(StringBuilder sb){
		this.sql.append(sb);
	}
	public void append(String sb){
		this.sql.append(sb);
	}
	
	public void addParameter(Object parameter){
		this.parameters.add(parameter);
	}
	
	public void line(int tabs){
		append(Clause.line(tabs));
	}

	public void tabs(int tabs){
		append(Clause.tabs(tabs));
	}

	public Object[] getParameters() {
		return parameters.toArray(new Object[parameters.size()]);
	}
}
