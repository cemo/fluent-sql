package com.ivanceras.fluent;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ 
		TestSQLBuilderDelete.class,
		TestSQLBuilderSelect.class,
		TestSQLBuilderInsert.class,
		TestSQLBuilderUpdate.class,
		TestComplexQuery.class,
		TestSQLBuilderFunctions.class,
		TestSQLBuilderMoreComplexFunctions.class,
		})
public class AllTests {

}
