package com.ivanceras.fluent;

import java.util.LinkedHashMap;
import java.util.LinkedList;

import com.ivanceras.fluent.Clause.Case;
import com.ivanceras.fluent.Clause.Condition;
import com.ivanceras.fluent.Clause.Distinct;
import com.ivanceras.fluent.Clause.DistinctOn;
import com.ivanceras.fluent.Clause.Except;
import com.ivanceras.fluent.Clause.Field;
import com.ivanceras.fluent.Clause.From;
import com.ivanceras.fluent.Clause.Function;
import com.ivanceras.fluent.Clause.GroupBy;
import com.ivanceras.fluent.Clause.Having;
import com.ivanceras.fluent.Clause.Intersect;
import com.ivanceras.fluent.Clause.Into;
import com.ivanceras.fluent.Clause.Join;
import com.ivanceras.fluent.Clause.Limit;
import com.ivanceras.fluent.Clause.Offset;
import com.ivanceras.fluent.Clause.On;
import com.ivanceras.fluent.Clause.OrderBy;
import com.ivanceras.fluent.Clause.Returning;
import com.ivanceras.fluent.Clause.Set;
import com.ivanceras.fluent.Clause.Type;
import com.ivanceras.fluent.Clause.Union;
import com.ivanceras.fluent.Clause.Update;
import com.ivanceras.fluent.Clause.Using;
import com.ivanceras.fluent.Clause.Values;
import com.ivanceras.fluent.Clause.Where;
import com.ivanceras.fluent.Clause.Window;
import com.ivanceras.fluent.Clause.With;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SQL{

	
	
	private static Logger log = LogManager.getLogger("SQL");
	
	protected LinkedHashMap<Integer, Object> callOrder = new LinkedHashMap<Integer, Object>();
	protected LinkedList<With> withQueries = new LinkedList<With>();
	protected Type type;
	protected LinkedList<Field> fields = new LinkedList<Field>();
	protected LinkedList<Function> functions = new LinkedList<Function>();
	protected LinkedList<Case> cases = new LinkedList<Case>();
	protected LinkedList<Set> set = new LinkedList<Set>();
	protected LinkedList<Distinct> distinctColumns = new LinkedList<Distinct>();
	//Distinct and Distinct ON be separated as they are a bit complex to handle
	protected LinkedList<DistinctOn> distinctOnColumns = new LinkedList<DistinctOn>();
	protected From from;//first table
	protected Update update;
	protected Into into;
	protected boolean doCommaField = false;

	protected LinkedList<Values> values = new LinkedList<Values>();

	protected LinkedList<Join> joins  = new LinkedList<Join>();

	protected LinkedList<Union> union = new LinkedList<Union>();
	protected LinkedList<Intersect> intersect = new LinkedList<Intersect>();
	protected LinkedList<Except> except = new LinkedList<Except>();


	protected Where where;

	protected LinkedList<OrderBy> orderBy = new LinkedList<OrderBy>();
	protected LinkedList<GroupBy> groupBy = new LinkedList<GroupBy>();
	protected Having having;

	protected LinkedList<Returning> returning = new LinkedList<Returning>();


	protected Limit limit;
	protected Offset offset;

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
			Distinct distinct = new Distinct(col);
			this.distinctColumns.add(distinct);
			called(distinct);
		}
		return this;
	}
	public SQL DISTINCT_ON(String... columns){
		for(String col : columns){
			DistinctOn distinctOn = new DistinctOn(col);
			this.distinctOnColumns.add(distinctOn);
			called(distinctOn);
		}
		return this;
	}
	public SQL FIELD(SQL sql){
		Field field = new Field(sql);
		field.fieldSql = sql;
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
	
	public SQL MAX(SQL sql){
		return function(Function.MAX, new Field(sql));
	}
	
	public SQL MAX(String column){
		return function(Function.MAX, new Field(column));
	}

	
	public SQL MIN(String column){
		return function(Function.MIN, new Field(column));
	}
	public SQL MIN(SQL sql){
		return function(Function.MIN, new Field(sql));
	}

	
	public SQL AVG(String column){
		return function(Function.AVG, new Field(column));
	}
	public SQL AVG(SQL sql){
		return function(Function.AVG, new Field(sql));
	}

	public SQL LOWER(String column){
		return function(Function.LOWER, new Field(column));
	}
	public SQL LOWER(SQL sql){
		return function(Function.LOWER, new Field(sql));
	}
	public SQL UPPER(String column){
		return function(Function.LOWER, new Field(column));
	}
	public SQL UPPER(SQL sql){
		return function(Function.LOWER, new Field(sql));
	}
	
	
	private SQL function(String functionName, Field field){
		Function function  = new Function(functionName, field);
		return function(function);
	}
	
	private SQL function(Function function){
		this.functions.add(function);
		return called(function);
	}
	
	public SQL AS(String columnAs){
		Object lasCall = getLastCall();
		if(lasCall instanceof Field){
			((Field)lasCall).columnAs = columnAs;
		}
		if(lasCall instanceof Function){
			((Function)lasCall).functionAs = columnAs;
		}
		return this;
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
	public SQL AND(String column1, String column2){
		return ON(column1, column2);
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
			Join lastJoin = getLastJoin();
			Using using = new Using(col);
			lastJoin.using.add(using);
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

	public SQL WHERE(String column){
		if(where == null){
			where = new Where();
		}
		Condition condition = new Condition(column);
		where.add(condition);
		return called(condition);
	}

	public SQL AND(String column){
		Condition condition = new Condition(column);
		where.and(condition);
		return called(condition);
	}
	public SQL OR(String column){
		Condition condition = new Condition(column);
		where.or(condition);
		return called(condition);
	}

	/**
	 * 
	 * @return the last condition appropriately, 
	 * when there is no having clause, 
	 * the last condition is from where clause
	 */
	private Condition getLastCondition(){
		//give priority to having clause first
		if(this.having != null && this.having.conditions.size() > 0){
			Condition lastHavingCond = this.having.conditions.get(this.having.conditions.size()-1);
			if(lastHavingCond instanceof Condition){
				return lastHavingCond;
			}
		}
		if(this.where != null && this.where.conditions.size() > 0){
			Condition lastWhereCond = this.where.conditions.get(this.where.conditions.size()-1);
			if(lastWhereCond instanceof Condition){
				return lastWhereCond;
			}
		}
		//TODO: return the last condition of the field
		return null;
	}

	public SQL EQUAL_TO(Object value){
		Condition condition = getLastCondition();
		condition.equality = Condition.EQUAL;
		condition.field2 = new Field(value);
		return called(condition);
	}
	public SQL EQUAL_TO_FIELD(String column) {
		Condition condition = getLastCondition();
		condition.equality = Condition.EQUAL;
		condition.field2 = new Field(column);
		return called(condition);
	}

	public SQL GREATER_THAN(Object value){
		Condition condition = getLastCondition();
		condition.equality = Condition.GREATER_THAN;
		condition.field2 = new Field(value);
		return called(condition);
	}
	public SQL GREATER_THAN_OR_EQUAL(Object value){
		Condition condition = getLastCondition();
		condition.equality = Condition.GREATER_THAN_OR_EQUAL;
		condition.field2 = new Field(value);
		return called(condition);
	}
	public SQL LESS_THAN(Object value){
		Condition condition = getLastCondition();
		condition.equality = Condition.LESS_THAN;
		condition.field2 = new Field(value);
		return called(condition);
	}
	public SQL LESS_THAN_OR_EQUAL(Object value){
		Condition condition = getLastCondition();
		condition.equality = Condition.LESS_THAN_OR_EQUAL;
		condition.field2 = new Field(value);
		return called(condition);
	}
	public SQL NOT_EQUAL_TO(Object value){
		Condition condition = getLastCondition();
		condition.equality = Condition.NOT_EQUAL;
		condition.field2 = new Field(value);
		return called(condition);
	}
	public SQL NOT_EQUAL_TO_FIELD(String column){
		Condition condition = getLastCondition();
		condition.equality = Condition.NOT_EQUAL;
		condition.field2 = new Field(column);
		return called(condition);
	}
	public SQL IS_NOT_NULL(){
		Condition condition = getLastCondition();
		condition.equality = Condition.IS_NOT_NULL;
		return called(condition);
	}
	public SQL IS_NULL(){
		Condition condition = getLastCondition();
		condition.equality = Condition.IS_NULL;
		return called(condition);
	}

	//	public SQL AND(String field, String operator, Object value){
	//		return WHERE(field, operator, value);
	//	}
	//	public SQL AND(String field, String operator, SQL sql){
	//		return WHERE(field, operator, sql);
	//	}

	public SQL IN(SQL sql){
		Object lastCall = getLastCall();
		if(lastCall instanceof Condition){
			Condition condition = (Condition) lastCall;
			condition.equality = Condition.IN;
			condition.field2 = new Field(sql);
			return called(condition);
		}
		else{
			Condition condition = new Condition();
			condition.equality = Condition.NOT_IN;
			condition.field2 = new Field(sql);
			Field field = new Field(condition);
			this.fields.add(field);
			return called(field);
		}
	}
	public SQL IN(Object... value){
		Object lastCall = getLastCall();
		if(lastCall instanceof Condition){
			Condition condition = (Condition) lastCall;
			condition.equality = Condition.IN;
			condition.field2 = new Field(value);
			return called(condition);
		}
		else{
			Condition condition = new Condition();
			condition.equality = Condition.IN;
			condition.field2 = new Field(value);
			Field field = new Field(condition);
			this.fields.add(field);
			return called(field);
		}
	}
	public SQL NOT_IN(Object... value){
		Object lastCall = getLastCall();
		if(lastCall instanceof Condition){
			Condition condition = (Condition) lastCall;
			condition.equality = Condition.NOT_IN;
			condition.field2 = new Field(value);
			return called(condition);
		}
		else{
			Condition condition = new Condition();
			condition.equality = Condition.NOT_IN;
			condition.field2 = new Field(value);
			Field field = new Field(condition);
			this.fields.add(field);
			return called(field);
		}
	}
	public SQL NOT_IN(SQL sql){
		Object lastCall = getLastCall();
		if(lastCall instanceof Condition){
			Condition condition = (Condition) lastCall;
			condition.equality = Condition.NOT_IN;
			condition.field2 = new Field(sql);
			return called(condition);
		}
		else{
			Condition condition = new Condition();
			condition.equality = Condition.NOT_IN;
			condition.field2 = new Field(sql);
			Field field = new Field(condition);
			this.fields.add(field);
			return called(field);
		}
	}

	public SQL EXIST(SQL sql){
		Object lastCall = getLastCall();
		if(lastCall instanceof Condition){
			Condition condition = (Condition) lastCall;
			condition.equality = Condition.EXISTS;
			condition.field2 = new Field(sql);
			return called(condition);
		}
		else{
			Condition condition = new Condition();
			condition.equality = Condition.EXISTS;
			condition.field2 = new Field(sql);
			Field field = new Field(condition);
			this.fields.add(field);
			return called(field);
		}
	}

	public SQL NOT_EXIST(SQL sql){
		Object lastCall = getLastCall();
		if(lastCall instanceof Condition){
			Condition condition = (Condition) lastCall;
			condition.equality = Condition.NOT_EXISTS;
			condition.field2 = new Field(sql);
			return called(condition);
		}
		else{
			Condition condition = new Condition();
			condition.equality = Condition.NOT_EXISTS;
			condition.field2 = new Field(sql);
			Field field = new Field(condition);
			this.fields.add(field);
			return called(field);
		}
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
	public SQL DESC(){
		if(this.orderBy.size() > 0){
			OrderBy lastOrderBy =  this.orderBy.getLast();
			lastOrderBy.asc = false;
			return called(lastOrderBy);
		}
		return null;
	}

	public SQL GROUP_BY(String... column){
		for(String col : column){
			GroupBy group = new GroupBy(col);
			this.groupBy.add(group);
			called(group);
		}
		return this;
	}
	public SQL GROUP_BY(SQL... sql){
		for(SQL sq : sql){
			GroupBy group = new GroupBy(sq);
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
	public SQL HAVING(String column1){
		Condition condition = new Condition(column1);
		Having having = new Having(condition);
		this.having  = having;
		return called(having);
	}

	public SQL WINDOW(String name, String expression){
		Window window = new Window(name, expression);
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


	public Breakdown build() {
		long t1 = System.nanoTime();
		Breakdown bk = new Breakdown();
		build(bk, 0, this);
		long t2 = System.nanoTime();
		long took = t2 - t1;
		log.trace("BUILDING SQL TOOK "+((float)took/1000000.0f)+" ms");
		return bk;
	}
	
	public void build(Breakdown bk, int tabs) {
		build(bk, tabs, this);
	}

	/**
	 * build all the complete SQL statement by assembling the fragments (clause statements)
	 */
	public void build(Breakdown bk, int tabs, SQL sql) {
		sql.doCommaField = false;
		buildClauseAllCTEs(bk, tabs, sql);
		buildClauseType(bk, tabs, sql);
		buildClauseAllDistinctOnColumns(bk, tabs, sql);
		buildClauseAllDistinctColumns(bk, tabs, sql);
		buildClauseAllFunctions(bk, tabs, sql);
		buildClauseAllFields(bk, tabs, sql);
		buildClauseInto(bk, tabs, sql);
		buildClauseUpdate(bk, tabs, sql);
		buildClauseAllSet(bk, tabs, sql);
		buildClauseFrom(bk, tabs, sql);
		buildClauseAllJoin(bk, tabs, sql);
		buildClauseWhere(bk, tabs, sql);
		buildClauseAllUnion(bk, tabs, sql);//TODO: find a resolution to whether where or union comes first
		buildClauseAllIntersect(bk, tabs, sql);
		buildClauseAllExcept(bk, tabs, sql);
		buildClauseAllGroupBy(bk, tabs, sql);
		buildClauseAllHaving(bk, tabs, sql);
		buildClauseAllValues(bk, tabs, sql);
		buildClauseAllOrderBy(bk, tabs, sql);
		buildClauseLimit(bk, tabs, sql);
		buildClauseOffset(bk, tabs, sql);

	}

	public void buildClauseAllCTEs(Breakdown bk, int tabs, SQL sql) {
		/////////////////////////////
		// build the CTE with query
		//////////////////////////////
		boolean doComma = false;
		for(With with : sql.withQueries){
			bk.append(Clause.line(tabs));
			if(doComma){
				bk.append(",");
			}else{
				bk.append(" WITH");
				doComma = true;
			}
			if(with.recursive){
				bk.append(" RECURSIVE");
			}
			with.build(bk, tabs);
		}
	}

	public void buildClauseType(Breakdown bk, int tabs, SQL sql) {
		if(sql.type != null){
			sql.type.build(bk, tabs);
		}
	}

	public void buildClauseAllDistinctOnColumns(Breakdown bk, int tabs, SQL sql) {
		//////////////////////////////
		// DISTINCT ON columns
		///////////////////////////////
		boolean doDistinctClause = true;
		boolean parenthesisOpened = false;
		int distinctIndex = 0;
		int totalDistinct = sql.distinctOnColumns.size();
		if(sql.doCommaField){
			bk.append(",");
		}
		boolean doComma = false;
		for(DistinctOn distinctOn : sql.distinctOnColumns){
			if(doComma){
				bk.append(",");
				bk.line(tabs+2);
			}else{
				doComma=true;
			}
			if(doDistinctClause){
				bk.append(" DISTINCT");
				doDistinctClause = false;
				bk.append(" ON");
				bk.append(" (");
				parenthesisOpened = true;
			}
			distinctOn.build(bk, tabs);
			if(parenthesisOpened && distinctIndex == totalDistinct - 1){
				bk.append(" )");
				parenthesisOpened = false;
			}
			distinctIndex++;
		}
	}
	
	public void buildClauseAllDistinctColumns(Breakdown bk, int tabs, SQL sql) {
		//////////////////////////////
		// DISTINCT columns
		///////////////////////////////
		boolean doDistinctClause = true;
		if(sql.doCommaField){
			bk.append(",");
		}
		boolean doComma = false;
		for(Distinct distinct : sql.distinctColumns){
			if(doComma){
				bk.append(",");
				bk.line(tabs+2);
			}else{
				doComma=true;
			}
			if(doDistinctClause){
				bk.append(" DISTINCT");
				doDistinctClause = false;
			}
			distinct.build(bk, tabs);
		}
	}

	public void buildClauseAllFunctions(Breakdown bk, int tabs, SQL sql) {
		for(Function function : sql.functions){
			if(sql.doCommaField){
				bk.append(",");
			}else{sql.doCommaField=true;}
			function.build(bk, tabs);
		}
	}

	public void buildClauseAllFields(Breakdown bk, int tabs, SQL sql) {
		for(Field field : sql.fields){
			if(sql.doCommaField){
				bk.append(",");
			}else{sql.doCommaField=true;}
			field.build(bk, tabs);
		}
	}

	public void buildClauseInto(Breakdown bk, int tabs, SQL sql) {
		if(sql.into != null){
			sql.into.build(bk, tabs);
		}
	}

	public void buildClauseAllSet(Breakdown bk, int tabs, SQL sql) {
		/////////////////////////////////
		// SET clause
		///////////////////////////////
		boolean doSetClause = true;
		boolean doCommaSetClause = false;
		for(Set set : sql.set){
			if(doSetClause){
				bk.append(" SET");
				doSetClause = false;
			}
			if(doCommaSetClause){
				bk.append(",");
			}else{doCommaSetClause = true;}
			set.build(bk, tabs);
		}
	}

	public void buildClauseFrom(Breakdown bk, int tabs, SQL sql) {
		if(sql.from != null){
			bk.line(tabs+1);
			sql.from.build(bk, tabs);
		}
	}

	public void buildClauseAllJoin(Breakdown bk, int tabs, SQL sql) {
		for(Join join : sql.joins){
			bk.line(tabs+2);
			join.build(bk, tabs);
		}
	}

	public void buildClauseAllUnion(Breakdown bk, int tabs, SQL sql) {
		for(Union union: sql.union){
			bk.line(tabs+1);
			union.build(bk, tabs);
			bk.line(tabs+1);
		}
	}

	public void buildClauseAllIntersect(Breakdown bk, int tabs, SQL sql) {
		for(Intersect intersect : sql.intersect){
			bk.line(tabs);
			bk.line(tabs);
			intersect.build(bk, tabs);
			bk.line(tabs);
		}

	}

	public void buildClauseAllExcept(Breakdown bk, int tabs, SQL sql) {
		////////////////////////////////
		//// EXCEPT Clause
		///////////////////////////////

		for(Except except : sql.except){
			bk.line(tabs);
			except.build(bk,tabs);
		}
	}

	public void buildClauseWhere(Breakdown bk, int tabs, SQL sql){
		if(where != null){
			where.build(bk, tabs);
		}
	}

	
	public void buildClauseAllGroupBy(Breakdown bk, int tabs, SQL sql) {
		boolean doCommaGroupby = false;
		boolean doGroupByClause = true;
		for(GroupBy groupby : sql.groupBy){
			if(doGroupByClause){
				bk.append(" GROUP BY");
				doGroupByClause = false;
			}
			if(doCommaGroupby){
				bk.append(",");
			}else{doCommaGroupby = true;}
			groupby.build(bk, tabs);
		}
	}

	public void buildClauseAllHaving(Breakdown bk, int tabs, SQL sql) {
		if(having != null){
			having.build(bk, tabs);
		}
	}

	public void buildClauseAllValues(Breakdown bk, int tabs, SQL sql) {
		for(Values val : sql.values){
			if(bk.doComma){
				bk.append(",");
			}else{
				bk.doComma = true;
			}
			val.build(bk, tabs);
		}
	}

	public void buildClauseAllOrderBy(Breakdown bk, int tabs, SQL sql) {
		////////////////////////////
		// ORDER BY clause
		////////////////////////////
		boolean doOrderByClause = true;
		boolean doCommaOrderBy = false;
		for(OrderBy orderby : sql.orderBy){
			if(doOrderByClause){
				bk.line(tabs+2);
				bk.append(" ORDER BY");
				doOrderByClause = false;
			}
			if(doCommaOrderBy){
				bk.append(",");
			}else{
				doCommaOrderBy = true;
			}
			orderby.build(bk, tabs);
		}
	}

	public void buildClauseOffset(Breakdown bk, int tabs, SQL sql) {
		if(sql.offset != null){
			sql.offset.build(bk, tabs);
		}
	}

	public void buildClauseLimit(Breakdown bk, int tabs, SQL sql) {
		if(sql.limit != null){
			sql.limit.build(bk, tabs);
		}
	}


	public void buildClauseUpdate(Breakdown bk, int tabs, SQL sql) {
		if(sql.update != null){
			sql.update.build(bk, tabs);
		}
	}

}
