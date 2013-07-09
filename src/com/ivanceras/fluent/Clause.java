package com.ivanceras.fluent;

import java.util.LinkedList;

public class Clause {

	public interface ClauseBuild{

		void build(Breakdown bk, int tabs);

	}

	///////////////////////////////////
	//
	// SQL inner Class
	//
	////////////////////////////////////////

	public static class Type implements ClauseBuild{
		protected String type;
		public static final String SELECT = "SELECT";
		public static final String INSERT = "INSERT";
		public static final String UPDATE = "UPDATE";
		public static final String DELETE = "DELETE";

		public Type(String type){
			this.type = type;
		}

		@Override
		public void build(Breakdown bk, int tabs) {
			if(type.equals(Type.SELECT)){
				bk.append(tabs(tabs));
				bk.append(" SELECT");
			}else if(type.equals(Type.INSERT)){
				bk.append(tabs(tabs));
				bk.append(" INSERT");
			}else if(type.equals(Type.UPDATE)){
				bk.append(tabs(tabs));
				bk.append(" UPDATE");
			}else if(type.equals(Type.DELETE)){
				bk.append(tabs(tabs));
				bk.append(" DELETE");
			}
		}
	}

	public static class With implements ClauseBuild{
		protected String name;
		protected boolean recursive;
		protected SQL sql;

		public With(String name, SQL sql, boolean recursive){
			this.name = name;
			this.sql = sql;
			this.recursive = recursive;
		}

		@Override
		public void build(Breakdown bk, int tabs){
			bk.append(" "+name);
			bk.append(" AS ");
			if(sql != null){
				bk.append(" (");
				bk.append(line(tabs));
				bk.append(tabs(tabs+1));
				sql.build(bk, tabs);
				bk.append(line(tabs));
				bk.append(" )");
				bk.append(line(0));
			}
		}
	}

	public static class Distinct implements ClauseBuild{
		protected String column;
		protected SQL sql;
		public Distinct(String column){
			this.column = column;
		}
		public Distinct(SQL sql){
			this.sql = sql;
		}
		/**
		 * Not possible, implementing classes shoud have its own implementation
		 */
		@Override
		public void build(Breakdown bk, int tabs) {
			if(column != null){
				bk.append(" "+column);
			}
			if(sql != null){
				sql.build(bk, tabs);
			}
		}
	}
	public static class DistinctOn implements ClauseBuild{
		protected String column;
		protected SQL sql;
		public DistinctOn(String column){
			this.column = column;
		}
		public DistinctOn(SQL sql){
			this.sql = sql;
		}
		@Override
		public void build(Breakdown bk, int tabs) {
			if(column != null){
				bk.append(" "+column);
			}
			if(sql != null){
				sql.build(bk, tabs);
			}
		}
	}

	public static class Field implements ClauseBuild{
		protected String column;
		protected SQL fieldSql;
		protected Function function;
		protected String columnAs;
		protected LinkedList<Values> values = new LinkedList<Values>();
		protected Condition condition;//Condition can also be in field

		public Field(String field){
			this.column = field;
		}
		public Field(Function function){
			this.function = function;
		}
		public Field(Object... pvalues){
			for(Object val : pvalues){
				this.values.add(new Values(val));
			}
		}
		public Field(Condition condition){
			this.condition = condition;
		}
		public Field(SQL sql){
			this.fieldSql = sql;
		}
		public void build(Breakdown bk, int tabs){
			if(this.function != null){
				this.function.build(bk, tabs);
			}
			if(this.column != null){
				bk.append(" "+this.column);
			}
			boolean doCommaValues = false;

			if(values.size() > 1){
				bk.append(" (");
			}
			for(Values val : this.values){
				if(doCommaValues){
					bk.append(",");
				}else{
					doCommaValues = true;
				}
				val.build(bk, tabs);
			}
			if(values.size() > 1){
				bk.append(" )");
			}


			if(this.fieldSql != null){
				if(this.fieldSql.type != null){//Don't put parenthesis when it is not a complex query like SELECT,INSERT.
					bk.append(" (");//Non complex queries are just VALUES.. FUNCTIONS
				}
				fieldSql.build(bk, tabs);
				if(this.fieldSql.type != null){
					bk.append(" )");
				}
			}
			if(this.condition != null){
				this.condition.build(bk, tabs);
			}
			if(this.columnAs != null){
				bk.append(" AS");
				bk.append(" "+this.columnAs);
			}
		}
	}

	public static class Function implements ClauseBuild{
		public static final String MAX = "MAX";
		public static final String MIN = "MIN";
		public static final String COUNT = "COUNT";
		public static final String SUM = "SUM";
		public static final String AVG = "AVG";
		public static final String LOWER = "LOWER";
		public static final String UPPER = "UPPER";
		protected String function;
		protected LinkedList<Field> functionFields = new LinkedList<Field>();
		protected String functionAs;

//		public Function(String function, Field field){
//			this.function = function;
//			this.functionFields.add(field);
//		}
		public Function(String function, Field... field){
			this.function = function;
			for(Field f : field){
				this.functionFields.add(f);
			}
		}

		public void build(Breakdown bk, int tabs){
			boolean fieldFunctionOpenedParenthesis = false;
			if(function != null){
				bk.append(" "+function);
				bk.append(" (");
				fieldFunctionOpenedParenthesis = true;
			}
			boolean doCommaField = false;
			for(Field field : functionFields){
				if(doCommaField){
					bk.append(",");
				}else{doCommaField=true;}
				field.build(bk,tabs);
			}
			if(fieldFunctionOpenedParenthesis){
				bk.append(" )");
				fieldFunctionOpenedParenthesis = false;
			}
			if(functionAs != null){
				bk.append(" AS "+functionAs);
			}
		}
	}



	public static class Set implements ClauseBuild{
		protected String column;
		protected Object value;
		public Set(String column){
			this.column = column;
		}
		public Set(String column, Object value){
			this.column = column;
			this.value = value;
		}
		@Override
		public void build(Breakdown bk, int tabs) {
			bk.append(" "+column);
			bk.append(" =");
			bk.append(" ?");
			bk.addParameter(value);
		}
	}

	public static class Case implements ClauseBuild{
		protected When when;
		protected Then then;
		protected Else elseCase;

		public static class When{
			protected String expression;
			protected SQL sql;
			public When(String expression){
				this.expression = expression;
			}
			public When(SQL sql){
				this.sql = sql;
			}
		}
		public static class Then{
			protected String expression;
			protected SQL sql;
			public Then(String expression){
				this.expression = expression;
			}
			public Then(SQL sql){
				this.sql = sql;
			}

		}
		public static class Else{
			protected String expression;
			protected SQL sql;
			public Else(String expression){
				this.expression = expression;
			}
			public Else(SQL sql){
				this.sql = sql;
			}
		}
		@Override
		public void build(Breakdown bk, int tabs) {

		}

	}


	public static class From implements ClauseBuild{
		protected String table;
		public From(String table){
			this.table = table;
		}
		@Override
		public void build(Breakdown bk, int tabs) {
			bk.append(" FROM");
			bk.append(" "+table);
		}
	}
	public static class Into implements ClauseBuild{
		protected String table;
		public Into(String table){
			this.table = table;
		}
		@Override
		public void build(Breakdown bk, int tabs) {
			bk.append(line(tabs+1));
			bk.append(" INTO");
			bk.append(" "+this.table);
		}
	}
	public static class Update implements ClauseBuild{
		protected String table;
		public Update(String table){
			this.table = table;
		}
		@Override
		public void build(Breakdown bk, int tabs) {
			bk.append(" "+this.table);
		}
	}

	public static class Values implements ClauseBuild{
		protected Object singleValue;
		protected SQL sql;
		public Values(Object value){
			this.singleValue = value;
		}
		public Values(SQL sql){
			this.sql = sql;
		}
		@Override
		public void build(Breakdown bk, int tabs) {
			if(singleValue != null){
				bk.append(" ?");
				bk.addParameter(singleValue);
			}
			if(sql != null){
				bk.append(" (");
				sql.build(bk, tabs);
				bk.append(" )");
			}
		}
	}

	public static class Join implements ClauseBuild{
		public static final String INNER_JOIN = "INNER JOIN";
		public static final String LEFT_JOIN = "LEFT JOIN";
		public static final String LEFT_OUTER_JOIN = "LEFT OUTER JOIN";
		public static final String RIGHT_JOIN = "RIGHT JOIN";
		public static final String RIGHT_OUTER_JOIN = "RIGHT OUTER JOIN";
		public static final String CROSS_JOIN = "CROSS JOIN";

		protected String joinType;
		protected String table;
		protected LinkedList<On> on = new LinkedList<On>();
		protected LinkedList<Using> using  = new LinkedList<Using>();



		public Join(String joinType, String table){
			this.joinType = joinType;
			this.table = table;
		}

		public void build(Breakdown bk, int tabs){
			bk.append(" "+joinType);
			bk.append(" "+table);
			buildUsing(bk, tabs);
			buildOn(bk, tabs);
		}

		public void buildOn(Breakdown bk, int tabs){
			boolean doOnJoinClause = true;
			for(On on : this.on){
				bk.append(line(tabs+2));
				if(doOnJoinClause){
					bk.append(" ON");
					doOnJoinClause = false;
				}else{
					bk.append(" AND");
				}
				on.build(bk, tabs);
			}
		}
		public void buildUsing(Breakdown bk, int tabs){
			///////////////////////////
			// USING
			///////////////////////////
			boolean doUsingClause = true;
			boolean doCommaUsing = false;
			for(Using using : this.using){
				if(doUsingClause){
					bk.append(line(tabs+2));
					bk.append(" USING");
					doUsingClause = false;
				}
				if(doCommaUsing){
					bk.append(",");
				}else{doCommaUsing = true;}
				using.build(bk, tabs);
			}
		}

	}

	public static class On implements ClauseBuild{
		protected String column1;
		protected String column2;
		public On(String column1){
			this.column1 = column1;
		}
		public On(String column1, String column2){
			this.column1 = column1;
			this.column2 = column2;
		}

		public void build(Breakdown bk, int tabs){
			bk.line(tabs+2);
			bk.append(" "+column1);
			bk.append(" = ");
			bk.append(" "+column2);
		}
	}
	public static class Using implements ClauseBuild{
		protected String column;
		public Using(String column){
			this.column = column;
		}
		@Override
		public void build(Breakdown bk, int tabs) {
			bk.append(" "+column);
		}
	}
	public static class Union implements ClauseBuild{
		protected boolean ALL;
		protected SQL sql;
		public Union(boolean ALL, SQL sql){
			this.ALL = ALL;
			this.sql = sql;
		}
		@Override
		public void build(Breakdown bk, int tabs) {
			bk.append(" UNION");
			if(ALL){
				bk.append(" ALL");
			}
			sql.build(bk, tabs);

		}
	}
	public static class Intersect implements ClauseBuild{
		protected boolean ALL;
		protected SQL sql;
		public Intersect(boolean ALL, SQL sql){
			this.ALL = ALL;
			this.sql = sql;
		}
		@Override
		public void build(Breakdown bk, int tabs) {
			bk.append(" INTERSECT");
			if(ALL){
				bk.append(" ALL");
			}
			sql.build(bk, tabs);
		}
	}
	public static class Except implements ClauseBuild{
		protected boolean ALL;
		protected SQL sql;
		public Except(boolean ALL, SQL sql){
			this.ALL = ALL;
			this.sql = sql;
		}
		@Override
		public void build(Breakdown bk, int tabs) {
			bk.append(" EXCEPT");
			if(ALL){
				bk.append(" ALL");
			}
			sql.build(bk, tabs);
		}
	}

	public static class Limit implements ClauseBuild{
		protected int limit;
		public Limit(int limit){
			this.limit = limit;
		}
		public int getLimit(){
			return this.limit;
		}
		@Override
		public void build(Breakdown bk, int tabs) {
			bk.append(" LIMIT");
			bk.append(" "+limit);
		}
	}

	public static class Offset implements ClauseBuild{
		protected int offset;
		public Offset(int offset){
			this.offset = offset;
		}
		@Override
		public void build(Breakdown bk, int tabs) {
			bk.append(" OFFSET");
			bk.append(" "+offset);
		}
	}

	public static class Condition implements ClauseBuild{

		protected String connector;
		protected Function function1;
		protected Function function2;
		protected Field field1;
		protected Field field2;
		protected String equality;

		public static final String LESS_THAN = "<";
		public static final String LESS_THAN_OR_EQUAL = "<=";
		public static final String EQUAL = "=";
		public static final String GREATER_THAN = ">";
		public static final String GREATER_THAN_OR_EQUAL = ">=";
		public static final String NOT_EQUAL = "!=";
		public static final String IN = "IN";
		public static final String NOT_IN = "NOT IN";
		public static final String EXISTS = "EXISTS";
		public static final String NOT_EXISTS = "NOT EXISTS";
		
		public static final String LIKE = "LIKE";
		public static final String NULL = "NULL";
		public static final String IS_NULL = "IS NULL";	
		public static final String IS_NOT_NULL = "IS NOT NULL";

		public final static String AND = "AND";
		public final static String OR = "OR";


		public Condition(){

		}
		public Condition(String column){
			this.field1 = new Field(column);
		}
		public Condition(Field field1, String operator, Field field2){
			this.equality = operator;
			this.field1 = field1;
			this.field2 = field2;
		}
		public Condition(String column, String operator, Object value){
			this.equality = operator;
			this.field1 = new Field(column);
			this.field2 = new Field(new Values(value));
		}
		public Condition(Function function1, String operator, Function function2){
			this.equality = operator;
			this.function1 = function1;
			this.function2 = function2;
		}

		public void build(Breakdown bk, int tabs){
			if(function1 != null){
				function1.build(bk, tabs);
			}
			if(field1 != null){
				field1.build(bk, tabs);
			}
			bk.append(" "+equality);
			if(function2 != null){
				function2.build(bk, tabs);
			}
			if(field2 != null){
				field2.build(bk, tabs);
			}
		}


	}

	public static class Where implements ClauseBuild{
		protected LinkedList<Condition> conditions = new LinkedList<Condition>();


		public void add(Condition condition){
			this.conditions.add(condition);
		}
		public void and(String column1){
			Condition condition = new Condition();
			condition.connector = Condition.AND;
			condition.field1 = new Field(column1);
			add(condition);
		}

		public void and(Condition condition){
			condition.connector = Condition.AND;
			add(condition);
		}
		public void or(Condition condition){
			condition.connector = Condition.OR;
			add(condition);
		}

		public void build(Breakdown bk, int tabs){
			boolean doWhere = true;
			for(Condition condition : conditions){
				if(doWhere){
					bk.line(tabs+1);
					bk.append(" WHERE");
					doWhere = false;
				}
				else{
					bk.line(tabs+1);
					bk.append(" "+condition.connector);
				}
				condition.build(bk, tabs);
			}
		}

	}

	public static class In implements ClauseBuild{
		protected LinkedList<String> values;
		protected boolean in;
		protected SQL inSql;

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
			this.inSql = sql;
			this.in = in;
		}

		@Override
		public void build(Breakdown bk, int tabs) {
			if(!in){
				bk.append(" NOT");
			}
			bk.append(" IN");
			bk.append(" (");
			inSql.build(bk, tabs);
			bk.append(" )");
		}
	}

	public static class OrderBy implements ClauseBuild{
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
		@Override
		public void build(Breakdown bk, int tabs) {
			bk.append(" "+column);
			if(asc){
				//sb.append(" ASC");
			}else{
				bk.append(" DESC");
			}
		}

	}
	public static class GroupBy implements ClauseBuild{
		protected String column;
		protected Field field;
		public GroupBy(String column){
			this.column = column;
		}
		public GroupBy(SQL sql){
			this.field = new Field(sql);
		}
		@Override
		public void build(Breakdown bk, int tabs) {
			if(column != null){
				bk.append(" "+column);
			}
			if(field != null){
				field.build(bk, tabs);
			}
		}

	}
	public static class Having implements ClauseBuild{
		protected LinkedList<Condition> conditions = new LinkedList<Condition>();
		public Having(Condition condition){
			this.conditions.add(condition);
		}
		@Override
		public void build(Breakdown bk, int tabs) {
			boolean doHaving = true;
			for(Condition cond : conditions){
				if(doHaving){
					bk.line(tabs);
					bk.append(" HAVING");
					doHaving = false;
				}
				cond.build(bk, tabs);
			}
		}
	}

	public static class Window implements ClauseBuild{
		protected String name;
		protected String column;
		protected String function;
		protected String expression;
		protected Over over;

		public static class PartitionBy{
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
		public static class Over implements ClauseBuild{
			protected PartitionBy partitionBy;
			protected String alias;
			protected Over(PartitionBy partitionBy, String alias){
				this.partitionBy = partitionBy;
				this.alias = alias;
			}
			@Override
			public void build(Breakdown bk, int tabs) {
				// TODO Auto-generated method stub
			}
		}

		public Window(String name, String expression){
			this.name = name;
			this.expression = expression;
		}

		@Override
		public void build(Breakdown bk, int tabs) {
			// TODO Auto-generated method stub
		}

	}


	public static class Returning implements ClauseBuild{
		protected String column;
		public Returning(String column){
			this.column = column;
		}
		@Override
		public void build(Breakdown bk, int tabs) {
			// TODO Auto-generated method stub
		}
	}

	public static String line(int tabs){
		return "\n"+tabs(tabs);
	}

	public static String tabs(int tabs){
		StringBuilder tstr = new StringBuilder();
		for(int i = 0; i < tabs; i++){
			tstr.append("\t");
		}
		return tstr.toString();
	}

}
