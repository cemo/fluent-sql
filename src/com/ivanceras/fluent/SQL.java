package com.ivanceras.fluent;


import java.util.LinkedHashMap;
import java.util.LinkedList;

import com.ivanceras.fluent.SQL.Window.PartitionBy;

public class SQL {

	private LinkedHashMap<Integer, Object> callOrder = new LinkedHashMap<Integer, Object>();
	protected LinkedList<With> withQueries = new LinkedList<With>();
	private Type type;
	private LinkedList<Field> fields = new LinkedList<Field>();
	private LinkedList<Function> functions = new LinkedList<Function>();
	private LinkedList<Case> cases = new LinkedList<Case>();
	private LinkedList<Set> set = new LinkedList<Set>();
	private LinkedList<Distinct> distinctColumns = new LinkedList<Distinct>();
	private From from;//first table
	private Update update;
	private Into into;

	private LinkedList<Values> values = new LinkedList<Values>();

	private LinkedList<Join> joins  = new LinkedList<Join>();
	private LinkedList<Using> using  = new LinkedList<Using>();

	private LinkedList<Union> union = new LinkedList<Union>();
	private LinkedList<Intersect> intersect = new LinkedList<Intersect>();
	private LinkedList<Except> except = new LinkedList<Except>();


	private LinkedList<Where> whereStatements = new LinkedList<Where>();
	private LinkedList<In> in = new LinkedList<In>();
	private LinkedList<Exist> exist = new LinkedList<Exist>();

	private LinkedList<OrderBy> orderBy = new LinkedList<OrderBy>();
	private LinkedList<GroupBy> groupBy = new LinkedList<GroupBy>();
	private LinkedList<Having> having = new LinkedList<Having>();

	private LinkedList<Window> window = new LinkedList<Window>();
	private LinkedList<PartitionBy> partitionBy = new LinkedList<PartitionBy>(); 
	private LinkedList<Returning> returning = new LinkedList<Returning>();


	private Limit limit;
	private Offset offset;

	public SQL(){

	}

	private SQL called(Object call){
		int nCall = callOrder.size();
		this.callOrder.put(nCall, call);
		return this;
	}

	public SQL WITH(String name, SQL sql){
		With with = new With(name,sql,false);
		withQueries.add(with);
		return called(with);
	}
	public SQL WITH_RECURSIVE(String name, SQL sql){
		With with = new With(name,sql,true);
		withQueries.add(with);
		return called(with);
	}

	public SQL SELECT_ALL(){
		return SELECT("*");
	}
	public SQL SELECT(){
		this.type = new Type(Type.SELECT);
		return called(type);
	}

	public String getType(){
		return type.type;
	}
	public SQL SELECT(String... columns){
		SELECT();
		for(String col : columns){
			FIELD(col);
		}
		return this;
	}
	public SQL UPDATE(String table){
		this.type = new Type(Type.UPDATE);
		called(type);
		this.update = new Update(table);
		return called(update);
	}
	public SQL INSERT(){
		this.type = new Type(Type.INSERT);
		return called(type);
	}
	public SQL DELETE(){
		this.type = new Type(Type.DELETE);
		return called(type);
	}

	public SQL DISTINCT(String... columns){
		for(String col : columns){
			Distinct distinct = new Distinct(col, false);
			this.distinctColumns.add(distinct);
			called(distinct);
		}
		return this;
	}
	public SQL DISTINCT_ON(String... columns){
		for(String col : columns){
			Distinct distinct = new Distinct(col, true);
			this.distinctColumns.add(distinct);
			called(distinct);
		}
		return this;
	}
	public SQL FIELD(SQL sql){
		Field field = new Field(sql);
		field.sql = sql;
		this.fields.add(field);
		called(field);
		return this;
	}
	public SQL FIELD(String... columns){
		for(String col : columns){
			Field field = new Field(col);
			this.fields.add(field);
			called(field);
		}
		return this;
	}
	public SQL SUM(SQL sql){
		return function(Function.SUM, new Field(sql));
	}

	public SQL SUM(String column){
		return function(Function.SUM, new Field(column));
	}
	public SQL COUNT(SQL sql){
		return function(Function.COUNT, new Field(sql));
	}
	public SQL COUNT(String column){
		return function(Function.COUNT, new Field(column));
	}
	public SQL MAX(String column){
		return function(Function.MAX, new Field(column));

	}
	public static SQL MAX(Field field){
		return new SQL().function(Function.MAX, field);
	}
	public SQL MIN(String column){
		return function(Function.MIN, new Field(column));
	}
	
	private SQL function(String functionName, Field field){
		Function function  = new Function(functionName, field);
		this.functions.add(function);
		return called(function);
	}
	public SQL AVG(String column){
		Function function  = new Function(Function.AVG, new Field(column));
		this.functions.add(function);
		return called(function);

	}
	public SQL AS(String columnAs){
		Field lastField = getLastField();
		Object lasCall = getLastCall();
		System.err.println("lastcall: "+lasCall);
		System.err.println("lastField: "+lastField);
		if(lasCall instanceof Field){
			((Field)lasCall).columnAs = columnAs;
		}
		if(lasCall instanceof Function){
			((Function)lasCall).functionAs = columnAs;
		}
		return this;
	}

	private Field getLastField(){
		int size = fields.size();
		if(size < 1){
			return null;
		}
		Field lastField = fields.get(size-1);
		return lastField;
	}
	private Join getLastJoin(){
		int calls = callOrder.size();
		if(calls < 1){
			return null;
		}
		for(int i = calls; i >= 0; i--){
			Object lastCall = callOrder.get(i);
			if(lastCall instanceof Join){
				return ((Join)lastCall);
			}
		}
		return null;
	}

	private Object getLastCall(){
		int size = callOrder.size();
		if(size < 1){
			return null;
		}
		Object lastCall = callOrder.get(size - 1);
		return lastCall;
	}

	public SQL FROM(String table){
		this.from = new From(table);
		return called(from);
	}

	public SQL LEFT_JOIN(String table){
		Join join = new Join(Join.LEFT_JOIN, table);
		this.joins.add(join);
		return called(join);
	}

	public SQL RIGHT_JOIN(String table){
		Join join = new Join(Join.RIGHT_JOIN, table);
		this.joins.add(join);
		return called(join);
	}

	public SQL INNER_JOIN(String table){
		Join join = new Join(Join.INNER_JOIN, table);
		this.joins.add(join);
		return called(join);
	}

	public SQL CROSS_JOIN(String table){
		Join join = new Join(Join.CROSS_JOIN, table);
		this.joins.add(join);
		return called(join);
	}
	public SQL LEFT_OUTER_JOIN(String table){
		Join join = new Join(Join.LEFT_OUTER_JOIN, table);
		this.joins.add(join);
		return called(join);
	}
	public SQL RIGHT_OUTER_JOIN(String table){
		Join join = new Join(Join.RIGHT_OUTER_JOIN, table);
		this.joins.add(join);
		return called(join);
	}

	public SQL ON(String column1){
		On on = new On(column1);
		return on(on);
	}

	public SQL ON(String column1, String column2){
		On on = new On(column1, column2);
		return on(on);
	}
	
	private SQL on(On on){
		Join lastJoin = getLastJoin();
		lastJoin.on.add(on);
		return called(on);
	}
	/**
	 * And used in join on context
	 * Careful, might be confused with AND in Where
	 * @param column1
	 * @param field2
	 * @return
	 */
	public SQL AND(String arg1, String arg2){
		Object lastClause = getLastClauseCallWhetherWhereOrOn();
		if(lastClause instanceof On){
			ON(arg1, arg2);
		}
		if(lastClause instanceof Where){
			WHERE(arg1, arg2);
		}
		return this;
	}

	public Object getLastClauseCallWhetherWhereOrOn(){
		int calls = callOrder.size();
		for(int i = calls; i >= 0; i--){
			Object lastCall = callOrder.get(i);
			if(lastCall instanceof Where){
				return lastCall;
			}
			if(lastCall instanceof On){
				return lastCall;
			}
		}
		return null;
	}

	public SQL USING(String... column){
		for(String col : column){
			Using using = new Using(col);
			this.using.add(using);
			called(using);
		}
		return this;
	}

	public SQL UNION(SQL sql){
		Union union = new Union(false, sql);
		this.union.add(union);
		return called(union);
	}

	public SQL UNION_ALL(SQL sql){
		Union union = new Union(true, sql);
		this.union.add(union);
		return called(union);
	}

	public SQL INTERSECT(SQL sql){
		Intersect intersect = new Intersect(false, sql);
		this.intersect.add(intersect);
		return called(intersect);
	}
	public SQL INTERSECT_ALL(SQL sql){
		Intersect intersect = new Intersect(true, sql);
		this.intersect.add(intersect);
		return called(intersect);
	}

	public SQL EXCEPT(SQL sql){
		Except except = new Except(false, sql);
		this.except.add(except);
		return called(except);
	}

	public SQL EXCEPT_ALL(SQL sql){
		Except except = new Except(true, sql);
		this.except.add(except);
		return called(except);
	}

	/**
	 * Use in insert, insert INTO
	 * @param table
	 * @return
	 */
	public SQL INTO(String table){
		this.into = new Into(table);
		return called(into);
	}

	/**
	 * Used in insert statement, insert ... VALUE
	 * @param dao
	 * @return
	 */
	public SQL VALUE(SQL sql){
		Values value = new Values(sql);
		this.values.add(value);
		return called(value);
	}
	public SQL VALUE(Object objValue){
		Values value = new Values(objValue);
		this.values.add(value);
		return called(value);
	}
	public SQL VALUES(Object... objValue){
		for(Object objv : objValue){
			VALUE(objv);
		}
		return this;
	}

	/**
	 * Used in Update statement, update... SET column1 = value1
	 * @param field
	 * @param value
	 * @return
	 */
	public SQL SET(String field, Object value){
		Set set = new Set(field, value);
		this.set.add(set);
		return called(set);
	}

	public SQL OR(String field, String operator, Object value){
		Where where = new Where(field, operator, value);
		where.connector = Where.OR;
		this.whereStatements.add(where);
		return called(where);
	}

	public SQL OR(String expression){
		Where where = new Where(expression);
		where.connector = Where.OR;
		this.whereStatements.add(where);
		return called(whereStatements);
	}

	public SQL WHERE(String field, String operator, Object value){
		Where where = new Where(field, operator, value);
		this.whereStatements.add(where);
		return called(where);
	}
	public SQL WHERE(String field, String operator){
		Where where = new Where(field, operator);
		this.whereStatements.add(where);
		return called(where);
	}
	public SQL WHERE_NOT_NULL(String field){
		return WHERE(field, Where.IS_NOT_NULL);
	}
	public SQL WHERE_NULL(String field){
		return WHERE(field, Where.IS_NULL);
	}
	public SQL WHERE(String field, String operator, SQL sql){
		Where where = new Where(field, operator, sql);
		this.whereStatements.add(where);
		return called(where);
	}

	public SQL WHERE(String column){
		Where where = new Where(column);
		this.whereStatements.add(where);
		return called(where);
	}
	public SQL AND(String column){
		return WHERE(column);
	}

	public SQL EQUAL_TO(Object value){
		return equality(Where.EQUAL, value);
	}
	public SQL EQUAL_TO_FIELD(String column) {
		return equality(Where.EQUAL, new Field(column));
	}

	public SQL GREATER_THAN(Object value){
		return equality(Where.GREATER_THAN, value);
	}
	public SQL GREATER_THAN_OR_EQUAL(Object value){
		return equality(Where.GREATER_THAN_OR_EQUAL, value);
	}
	public SQL LESS_THAN(Object value){
		return equality(Where.LESS_THAN, value);
	}
	public SQL LESS_THAN_OR_EQUAL(Object value){
		return equality(Where.LESS_THAN_OR_EQUAL, value);
	}
	public SQL NOT_EQUAL_TO(Object value){
		return equality(Where.NOT_EQUAL, value);
	}
	public SQL IS_NOT_NULL(){
		return equality(Where.IS_NOT_NULL);
	}
	public SQL IS_NULL(){
		return equality(Where.IS_NULL);
	}

	private SQL equality(String equality){
		return equality(equality, null);
	}

	private SQL equality(String equality, Object value){
		Object lastCall = getLastClauseCallWhetherWhereOrOn();
		if(lastCall instanceof Where){
			Where where = ((Where)lastCall);
			where.operator = equality;
			where.value = value;
		}
		if(lastCall instanceof On){
			On on = ((On)lastCall);
			on.column2 = (String)value;
		}
		return this;
	}
	
	private SQL equality(String equality, Field column){
		Object lastCall = getLastClauseCallWhetherWhereOrOn();
		if(lastCall instanceof Where){
			Where where = ((Where)lastCall);
			where.operator = equality;
			where.field2 = column;
		}
		return this;
	}


	public SQL AND(String field, String operator, Object value){
		return WHERE(field, operator, value);
	}
	public SQL AND(String field, String operator, SQL sql){
		return WHERE(field, operator, sql);
	}

	/**
	 * As much as possible, don't use this
	 * @param expression
	 * @return
	 */
	@Deprecated
	public SQL IN(String... expression){
		In in = new In(expression);
		this.in.add(in);
		return called(in);
	}
	public SQL IN(SQL sql){
		In in = new In(sql);
		this.in.add(in);
		return called(in);
	}
	public SQL NOT_IN(SQL sql){
		In in = new In(sql, false);
		this.in.add(in);
		return called(in);
	}

	public SQL EXIST(SQL sql){
		Exist exist = new Exist(sql);
		this.exist.add(exist);
		return called(exist);
	}

	public SQL NOT_EXIST(SQL sql){
		Exist exist = new Exist(sql);
		this.exist.add(exist);
		return called(exist);
	}

	public SQL RETURNING(String column){
		Returning returning = new Returning(column);
		this.returning.add(returning);
		return called(returning);
	}

	public SQL ORDER_BY(String... field){
		for(String f : field){
			OrderBy orderby = new OrderBy(f);
			this.orderBy.add(orderby);
			called(orderby);
		}
		return this;
	}

	public SQL ORDER_BY_DESC(String... field){
		for(String f : field){
			OrderBy orderby = new OrderBy(f, false);
			this.orderBy.add(orderby);
			called(orderby);
		}
		return this;
	}

	public SQL GROUP_BY(String... column){
		for(String col : column){
			GroupBy group = new GroupBy(col);
			this.groupBy.add(group);
			called(group);
		}
		return this;
	}
	/**
	 * As much as possible, don't use this
	 * @param expression
	 * @return
	 */
	@Deprecated
	public SQL HAVING(String expression){
		Having having = new Having(expression);
		this.having.add(having);
		return called(having);
	}

	public SQL WINDOW(String name, String expression){
		Window window = new Window(name, expression);
		this.window.add(window);
		return called(window);
	}

	public SQL LIMIT(int limit){
		this.limit = new Limit(limit);
		return called(limit);
	}

	public SQL OFFSET(int offset){
		this.offset =new Offset(offset);
		return called(offset);
	}

	///////////////////////////////////
	//
	// SQL inner Class
	//
	////////////////////////////////////////

	public class Type{
		protected String type;
		public static final String SELECT = "SELECT";
		public static final String INSERT = "INSERT";
		public static final String UPDATE = "UPDATE";
		public static final String DELETE = "DELETE";

		public Type(String type){
			this.type = type;
		}
	}

	public class With{
		protected String name;
		protected boolean recursive;
		protected SQL sql;

		public With(String name, SQL sql, boolean recursive){
			this.name = name;
			this.sql = sql;
			this.recursive = recursive;
		}
	}

	public class Distinct{
		protected boolean ON;
		protected String column;
		protected SQL sql;
		public Distinct(String column, boolean ON){
			this.column = column;
			this.ON = ON;
		}
		public Distinct(SQL sql, boolean ON){
			this.sql = sql;
			this.ON = ON;
		}
		public Distinct(SQL sql){
			this.sql = sql;
		}
	}

	public class Field{
		protected String column;
		protected SQL sql;
		protected String columnAs;
		//TODO: add Value field argument as well
		public Field(String field){
			this.column = field;
		}
		public Field(SQL sql){
			this.sql = sql;
		}
	}
	
	public class Function{
		public static final String MAX = "MAX";
		public static final String MIN = "MIN";
		public static final String COUNT = "COUNT";
		public static final String SUM = "SUM";
		public static final String AVG = "AVG";
		protected String function;
		protected LinkedList<Field> functionFields = new LinkedList<Field>();
		protected String functionAs;
		
		public Function(String function, Field field){
			this.function = function;
			this.functionFields.add(field);
		}
		public Function(String function, Field... field){
			this.function = function;
			for(Field f : field){
				this.functionFields.add(f);
			}
		}
	}
	
	
	
	public class Set{
		protected String column;
		protected Object value;
		public Set(String column){
			this.column = column;
		}
		public Set(String column, Object value){
			this.column = column;
			this.value = value;
		}
	}

	public class Case{
		protected When when;
		protected Then then;
		protected Else elseCase;

		public class When{
			protected String expression;
			protected SQL sql;
			public When(String expression){
				this.expression = expression;
			}
			public When(SQL sql){
				this.sql = sql;
			}
		}
		public class Then{
			protected String expression;
			protected SQL sql;
			public Then(String expression){
				this.expression = expression;
			}
			public Then(SQL sql){
				this.sql = sql;
			}

		}
		public class Else{
			protected String expression;
			protected SQL sql;
			public Else(String expression){
				this.expression = expression;
			}
			public Else(SQL sql){
				this.sql = sql;
			}
		}

	}


	public class From{
		protected String table;
		public From(String table){
			this.table = table;
		}
	}
	public class Into{
		protected String table;
		public Into(String table){
			this.table = table;
		}
	}
	public class Update{
		protected String table;
		public Update(String table){
			this.table = table;
		}
	}

	public class Values{
		protected Object singleValue;
		protected SQL sql;
		public Values(Object value){
			this.singleValue = value;
		}
		public Values(SQL sql){
			this.sql = sql;
		}
	}

	public class Join{
		public static final String INNER_JOIN = "INNER JOIN";
		public static final String LEFT_JOIN = "LEFT JOIN";
		public static final String LEFT_OUTER_JOIN = "LEFT OUTER JOIN";
		public static final String RIGHT_JOIN = "RIGHT JOIN";
		public static final String RIGHT_OUTER_JOIN = "RIGHT OUTER JOIN";
		public static final String CROSS_JOIN = "CROSS JOIN";

		protected String joinType;
		protected String table;
		protected LinkedList<On> on = new LinkedList<On>();


		public Join(String joinType, String table){
			this.joinType = joinType;
			this.table = table;
		}

	}

	public class On{
		protected String column1;
		protected String column2;
		public On(String column1){
			this.column1 = column1;
		}
		public On(String column1, String column2){
			this.column1 = column1;
			this.column2 = column2;
		}
	}
	public class Using{
		protected String column;
		public Using(String column){
			this.column = column;
		}
	}
	public class Union{
		protected boolean ALL;
		protected SQL sql;
		public Union(boolean ALL, SQL sql){
			this.ALL = ALL;
			this.sql = sql;
		}
	}
	public class Intersect{
		protected boolean ALL;
		protected SQL sql;
		public Intersect(boolean ALL, SQL sql){
			this.ALL = ALL;
			this.sql = sql;
		}
	}
	public class Except{
		protected boolean ALL;
		protected SQL sql;
		public Except(boolean ALL, SQL sql){
			this.ALL = ALL;
			this.sql = sql;
		}
	}

	public class Limit{
		protected int limit;
		public Limit(int limit){
			this.limit = limit;
		}
	}

	public class Offset{
		protected int offset;
		public Offset(int offset){
			this.offset = offset;
		}
	}

	public class Where{
		protected String connector;
		protected Field field1;
		protected String operator;
		protected Object value;
		protected SQL sql;
		public Field field2;//When equating columns
		//		protected String expression;
		protected LinkedList<String> functionColumn = new LinkedList<String>();
		protected LinkedList<String> functionValue = new LinkedList<String>();
		protected LinkedList<Where> nestedWhere;

		public static final String LESS_THAN = "<";
		public static final String LESS_THAN_OR_EQUAL = "<=";
		public static final String EQUAL = "=";
		public static final String GREATER_THAN = ">";
		public static final String GREATER_THAN_OR_EQUAL = ">=";
		public static final String NOT_EQUAL = "!=";
		public static final String IN = "IN";
		public static final String NOT_IN = "NOT IN";
		public static final String LIKE = "LIKE";
		//		public static final String NOT_NULL = "NOT NULL";
		public static final String NULL = "NULL";
		public static final String IS_NULL = "IS NULL";	
		public static final String IS_NOT_NULL = "IS NOT NULL";

		public final static String AND = "AND";
		public final static String OR = "OR";

		public Where(String column, String operator, Object value){
			this(column, operator);
			this.value = value;
		}
		
		public Where(String column, String operator, SQL sql){
			this(column,operator);
			this.sql = sql;
		}

		public Where(String column, String operator){
			this(column);
			this.operator = operator;
		}



		public Where(String column, SQL sql){
			this(column);
			this.sql = sql;
		}

		public Where(String column){
			this.field1 = new Field(column);
		}

		public Where and(Where where){
			where.connector = AND;
			nestedWhere.add(where);
			return this;
		}
		public Where or(Where where){
			where.connector = OR;
			nestedWhere.add(where);
			return this;
		}

		public void setColumnFunction(String... function){
			for(String f : function){
				this.functionColumn.add(f);
			}
		}
		public void setValueFunction(String... function){
			for(String f : function){
				this.functionValue.add(f);
			}
		}


	}

	public class In{
		protected LinkedList<String> values;
		protected boolean in;
		protected SQL sql;

		public In(String[] value){
			this(value, true);
		}

		public In(String[] value, boolean in){
			for(String v : value){
				values.add(v);
			}
			this.in = in;
		}
		public In(SQL sql){
			this(sql, true);
		}
		public In(SQL sql, boolean in){
			this.sql = sql;
			this.in = in;
		}
	}

	public class Exist{
		protected SQL sql;
		protected boolean exist;

		public Exist(SQL sql){
			this(sql, true);
		}

		public Exist(SQL sql, boolean exist){
			this.sql = sql;
			this.exist = exist;
		}
	}

	public class OrderBy{
		protected String column;
		protected boolean asc;
		public OrderBy(String column){
			this.column = column;
			this.asc = true;
		}
		public OrderBy(String column, boolean asc){
			this.column = column;
			this.asc = asc;
		}

	}
	public class GroupBy{
		protected String column;
		public GroupBy(String column){
			this.column = column;
		}

	}
	public class Having{
		protected String condition;
		public Having(String condition){
			this.condition = condition;
		}
	}

	public class Window{
		protected String name;
		protected String column;
		protected String function;
		protected String expression;
		protected Over over;

		public class PartitionBy{
			protected String column;
			protected OrderBy orderBy;
			public PartitionBy(String column, OrderBy orderBy){
				this.column = column;
				this.orderBy = orderBy;
			}
			public PartitionBy(String column){
				this.column = column;
			}
		}
		public class Over{
			protected PartitionBy partitionBy;
			protected String alias;
			protected Over(PartitionBy partitionBy, String alias){
				this.partitionBy = partitionBy;
				this.alias = alias;
			}
		}

		public Window(String name, String expression){
			this.name = name;
			this.expression = expression;
		}

	}


	public class Returning{
		protected String column;
		public Returning(String column){
			this.column = column;
		}
	}

	public class Breakdown{

		public String sql;
		public Object[] parameters;
		public Breakdown(String sql, Object[] parameters){
			this.sql = sql;
			this.parameters = parameters;
		}
	}
	
	/**
	 * Build this SQL object
	 * @return
	 */
	public Breakdown build(){
		return build(this, 0);
	}
	
	/**
	 * Warning: Use the the protected fields from sql object, or else would end up using the class instance variable which would have a different parameters
	 * @param sql
	 * @param tabs
	 * @return
	 */
	private Breakdown build(SQL sql, int tabs){
		LinkedList<Object> parameters = new LinkedList<Object>(); 
		StringBuilder sb = new StringBuilder();


		/////////////////////////////
		// build the WTE with query
		//////////////////////////////
		for(With with : sql.withQueries){
			sb.append(line(tabs));
			sb.append(" WITH");
			if(with.recursive){
				sb.append(" RECURSIVE");
			}
			sb.append(" "+with.name);
			sb.append(" AS ");
			if(with.sql != null){
				Breakdown withBreakdown = build(with.sql, tabs);
				sb.append(" (");
				sb.append(line(tabs));
				sb.append(tabs(tabs+1)+" "+withBreakdown.sql);
				for(Object p : withBreakdown.parameters){
					parameters.add(p);
				}
				sb.append(line(tabs));
				sb.append(" )");
				sb.append(line(0));
			}
		}
		if(sql.type != null){
			if(sql.type.type.equals(Type.SELECT)){
				sb.append(tabs(tabs));
				sb.append(" SELECT");
			}else if(sql.type.type.equals(Type.INSERT)){
				sb.append(tabs(tabs));
				sb.append(" INSERT");
			}else if(sql.type.type.equals(Type.UPDATE)){
				sb.append(tabs(tabs));
				sb.append(" UPDATE");
			}else if(sql.type.type.equals(Type.DELETE)){
				sb.append(tabs(tabs));
				sb.append(" DELETE");
			}
		}
		//////////////////////////////
		// DISTINCT columns
		///////////////////////////////
		boolean doCommaDistinct = false;
		boolean doDistinctClause = true;
		boolean doOnDistinctClause = true;
		boolean parenthesisOpened = false;
		int distinctIndex = 0;
		int totalDistinct = sql.distinctColumns.size();
		for(Distinct distinct : sql.distinctColumns){
			if(doCommaDistinct){
				sb.append(",");
				sb.append(line(tabs+2));
			}else{
				doCommaDistinct=true;
			}
			if(doDistinctClause){
				sb.append(" DISTINCT");
				doDistinctClause = false;
			}
			if(distinct.ON){
				if(doOnDistinctClause){
					sb.append(" ON");
					doOnDistinctClause = false;
					sb.append(" (");
					parenthesisOpened = true;
				}
			}
			if(distinct.column != null){
				sb.append(" "+distinct.column);
			}
			if(parenthesisOpened && distinctIndex == totalDistinct - 1){
				sb.append(" )");
				parenthesisOpened = false;
			}
			if(distinct.sql != null){
				Breakdown fieldBreakdown = build(distinct.sql, tabs);
				sb.append(" "+fieldBreakdown.sql);
				for(Object fieldParam : fieldBreakdown.parameters){
					parameters.add(fieldParam);
				}
			}
			distinctIndex++;
		}
		//////////////////////////
		/// build the functions
		///////////////////////////
		Breakdown functionBreakdown = buildFunctionList(tabs, sql.functions);
		sb.append(" "+functionBreakdown.sql);
		for(Object functionParam : functionBreakdown.parameters){
			parameters.add(functionParam);
		}

		/////////////////////////////
		/// build the fields
		/////////////////////////////
		Breakdown fieldsBreakdown = buildFieldList(tabs, sql.fields);
		sb.append(" "+fieldsBreakdown.sql);
		for(Object fieldsParam : fieldsBreakdown.parameters){
			parameters.add(fieldsParam);
		}
		///////////////////////////
		// INTO clause
		///////////////////////////
		if(sql.into != null){
			sb.append(line(tabs+1));
			sb.append(" INTO");
			sb.append(" "+sql.into.table);
		}
		///////////////////////////
		// UPDATE clause
		///////////////////////////
		if(sql.update != null){
			sb.append(line(tabs+1));
			//			sb.append(" UPDATE");
			sb.append(" "+sql.update.table);
		}

		/////////////////////////////////
		// SET clause
		///////////////////////////////
		boolean doSetClause = true;
		boolean doCommaSetClause = false;
		for(Set set : sql.set){
			if(doSetClause){
				sb.append(" SET");
				doSetClause = false;
			}
			if(doCommaSetClause){
				sb.append(",");
			}else{doCommaSetClause = true;}
			sb.append(" "+set.column);
			sb.append(" =");
			sb.append(" ?");
			parameters.add(set.value);
		}

		///////////////////////////
		// FROM clause
		///////////////////////////
		if(sql.from != null){
			sb.append(line(tabs+1));
			sb.append(" FROM");
			sb.append(" "+sql.from.table);
		}

		//////////////////////////
		// JOIN
		///////////////////////////
		for(SQL.Join join : sql.joins){
			sb.append(line(tabs+2));
			sb.append(" "+join.joinType);
			sb.append(" "+join.table);
			///////////////////////////
			// JOIN ON's
			///////////////////////////
			boolean doOnJoinClause = true;
			for(On on : join.on){
				sb.append(line(tabs+2));
				if(doOnJoinClause){
					sb.append(" ON");
					doOnJoinClause = false;
				}else{
					sb.append(" AND");
				}
				sb.append(" "+on.column1);
				sb.append(" = ");
				sb.append(" "+on.column2);
			}
		}

		///////////////////////////
		// USING
		///////////////////////////
		boolean doUsingClause = true;
		boolean doCommaUsing = false;
		for(Using using : sql.using){
			if(doUsingClause){
				sb.append(line(tabs+2));
				sb.append(" USING");
				doUsingClause = false;
			}
			if(doCommaUsing){
				sb.append(",");
			}else{doCommaUsing = true;}
			sb.append(" "+using.column);
		}
		///////////////////////////////
		/// WHERE clause
		///////////////////////////////
		boolean doWhere = true;
		for(Where where: sql.whereStatements){
			if(doWhere){
				sb.append(line(tabs+1));
				sb.append(" WHERE");
				doWhere = false;
			}
			else{
				sb.append(line(tabs+1));
				sb.append(" AND");
			}
			if(where.connector!=null){
				sb.append(line(tabs));
				sb.append(" "+where.connector);
			}
			if(where.field1 != null){
				Breakdown wherefieldBreakdown = buildField(tabs, where.field1, false);
				sb.append(" "+wherefieldBreakdown.sql);
				for(Object p : wherefieldBreakdown.parameters){
					parameters.add(p);
				}
			}
			if(where.operator != null){
				sb.append(" "+where.operator);
			}
			if(where.value != null){
				sb.append(" ?");
				parameters.add(where.value);
			}
			if(where.field2 != null){
				Breakdown wherefield2Breakdown = buildField(tabs, where.field2, false);
				sb.append(" "+wherefield2Breakdown.sql);
				for(Object p : wherefield2Breakdown.parameters){
					parameters.add(p);
				}
			}
			if(where.sql != null){
				sb.append(" (");
				sb.append(line(0));
				Breakdown whereBreakdown = build(where.sql,tabs+3);
				sb.append(whereBreakdown.sql);
				for(Object whereParam: whereBreakdown.parameters){
					parameters.add(whereParam);
				}
				sb.append(line(tabs+3));
				sb.append(")");
			}
			for(int i = 0; i < where.functionValue.size();i++){
				sb.append(")");
			}
		}

		///////////////////////////////////
		//  IN clause
		////////////////////////////////////
		for(In in : sql.in){
			if(!in.in){
				sb.append(" NOT");
			}
			sb.append(" IN");
			sb.append(" (");
			Breakdown inBreakdown = build(in.sql,tabs);
			sb.append(" "+inBreakdown.sql);
			for(Object p : inBreakdown.parameters){
				parameters.add(p);
			}
			sb.append(" )");
		}

		///////////////////////////
		/// UNION clause
		///////////////////////////

		for(Union union: sql.union){
			sb.append(line(tabs+1));
			sb.append(" UNION");
			if(union.ALL){
				sb.append(" ALL");
			}
			Breakdown unionBreakdown = build(union.sql,tabs);
			sb.append(line(tabs+1));
			sb.append(unionBreakdown.sql);
			for(Object p : unionBreakdown.parameters){
				parameters.add(p);
			}
		}

		////////////////////////////////
		//// INTERSECT Clause
		///////////////////////////////

		for(Intersect intersect : sql.intersect){
			sb.append(line(tabs));
			sb.append(" INTERSECT");
			if(intersect.ALL){
				sb.append(" ALL");
			}
			Breakdown intersectBreakdown = build(intersect.sql,tabs);
			sb.append(line(tabs));
			sb.append(intersectBreakdown.sql);
			for(Object p : intersectBreakdown.parameters){
				parameters.add(p);
			}
		}
		////////////////////////////////
		//// EXCEPT Clause
		///////////////////////////////

		for(Except except : sql.except){
			sb.append(line(tabs));
			sb.append(" EXCEPT");
			if(except.ALL){
				sb.append(" ALL");
			}
			Breakdown exceptBreakdown = build(except.sql,tabs);
			sb.append(line(tabs));
			sb.append(exceptBreakdown.sql);
			for(Object p : exceptBreakdown.parameters){
				parameters.add(p);
			}
		}

		///////////////////////////////////
		//   Group By
		///////////////////////////////////
		boolean doCommaGroupby = false;
		boolean doGroupByClause = true;
		for(GroupBy groupby : sql.groupBy){
			if(doGroupByClause){
				sb.append(" GROUP BY");
				doGroupByClause = false;
			}
			if(doCommaGroupby){
				sb.append(",");
			}else{doCommaGroupby = true;}
			sb.append(" "+groupby.column);
		}

		/////////////////////////////////////
		//   Having
		//////////////////////////////////////////

		//////////////////////////////
		// VALUES
		//////////////////////////////
		boolean doCommaValues = false;
		for(Values value : sql.values){
			if(value.singleValue != null){
				if(doCommaValues){
					sb.append(", ");
				}else{
					doCommaValues = true;
				}
				sb.append(" ?");
				parameters.add(value.singleValue);
			}
			if(value.sql != null){
				if(doCommaValues){
					sb.append(", ");
				}else{
					doCommaValues = true;
				}
				Breakdown valueBreakdown = build(value.sql,tabs);
				sb.append(valueBreakdown.sql);
				for(Object p : valueBreakdown.parameters){
					parameters.add(p);
				}
			}
		}
		////////////////////////////
		// ORDER BY clause
		////////////////////////////
		boolean doOrderByClause = true;
		boolean doCommaOrderBy = false;
		for(OrderBy orderby : sql.orderBy){
			if(doOrderByClause){
				sb.append(line(tabs+2));
				sb.append(" ORDER BY");
				doOrderByClause = false;
			}
			if(doCommaOrderBy){
				sb.append(",");
			}else{
				doCommaOrderBy = true;
			}
			sb.append(" "+orderby.column);
			if(orderby.asc){
				//sb.append(" ASC");
			}else{
				sb.append(" DESC");
			}
		}
		///////////////////////////////
		// LIMIT
		/////////////////////////////////
		if(sql.limit != null){
			sb.append(" LIMIT");
			sb.append(" "+sql.limit.limit);
		}
		///////////////////////////////
		// OFFSET
		/////////////////////////////////
		if(sql.offset != null){
			sb.append(" OFFSET");
			sb.append(" "+sql.offset.offset);
		}

		Breakdown breakdown = new Breakdown(sb.toString(), parameters.toArray(new Object[parameters.size()]));
		return breakdown;
	}

	private String line(int tabs){
		return "\n"+tabs(tabs);
	}

	private String tabs(int tabs){
		StringBuilder tstr = new StringBuilder();
		for(int i = 0; i < tabs; i++){
			tstr.append("\t");
		}
		return tstr.toString();
	}

	private Breakdown buildFieldList(int tabs, LinkedList<Field> fields){
		StringBuilder sb = new StringBuilder();
		LinkedList<Object> parameters = new LinkedList<Object>();
		boolean doCommaField = false;
		for(Field field : fields){
			if(doCommaField){
				sb.append(",");
				sb.append(line(tabs+2));
			}else{doCommaField=true;}
			
			Breakdown fieldBreakdown = buildField(tabs, field, doCommaField);
			sb.append(fieldBreakdown.sql);
			
			for(Object fieldParam : fieldBreakdown.parameters){
				parameters.add(fieldParam);
			}
		}
		Breakdown breakdown = new Breakdown(sb.toString(), parameters.toArray(new Object[parameters.size()]));
		return breakdown;
	}
	
	private Breakdown buildField(int tabs, Field field, boolean doCommaField){
		LinkedList<Object> parameters = new LinkedList<Object>();
		StringBuilder sb = new StringBuilder();
		if(field.column != null){
			sb.append(" "+field.column);
		}
		if(field.sql != null){
			Breakdown fieldBreakdown = build(field.sql, tabs);
			sb.append(" (");
			sb.append(" "+fieldBreakdown.sql);
			sb.append(" )");
			for(Object fieldParam : fieldBreakdown.parameters){
				parameters.add(fieldParam);
			}
		}
		if(field.columnAs != null){
			sb.append(" AS");
			sb.append(" "+field.columnAs);
		}
		Breakdown breakdown = new Breakdown(sb.toString(), parameters.toArray(new Object[parameters.size()]));
		return breakdown;
	}
	
	
	private Breakdown buildFunctionList(int tabs, LinkedList<Function> functions){
		StringBuilder sb = new StringBuilder();
		LinkedList<Object> parameters = new LinkedList<Object>();
		boolean doCommaFunction = false;
		for(Function function : functions){
			if(doCommaFunction){
				sb.append(",");
				sb.append(line(tabs+2));
			}else{doCommaFunction=true;}
			Breakdown fieldBreakdown = buildFunction(tabs, function, doCommaFunction);
			sb.append(fieldBreakdown.sql);
			for(Object fieldParam : fieldBreakdown.parameters){
				parameters.add(fieldParam);
			}
		}
		Breakdown breakdown = new Breakdown(sb.toString(), parameters.toArray(new Object[parameters.size()]));
		return breakdown;
	}
	
	private Breakdown buildFunction(int tabs, Function function, boolean doCommaFunction){
		StringBuilder sb = new StringBuilder();
		LinkedList<Object> parameters = new LinkedList<Object>();
		boolean fieldFunctionOpenedParenthesis = false;
		if(function.function != null){
			sb.append(" "+function.function);
			sb.append(" (");
			fieldFunctionOpenedParenthesis = true;
		}
		Breakdown fieldBreakdown = buildFieldList(tabs, function.functionFields);
		sb.append(" "+fieldBreakdown.sql);
		for(Object fieldParam : fieldBreakdown.parameters){
			parameters.add(fieldParam);
		}
		if(fieldFunctionOpenedParenthesis){
			sb.append(" )");
			fieldFunctionOpenedParenthesis = false;
		}
		if(function.functionAs != null){
			sb.append(" AS "+function.functionAs);
		}
		Breakdown breakdown = new Breakdown(sb.toString(), parameters.toArray(new Object[parameters.size()]));
		return breakdown;
	}


}
