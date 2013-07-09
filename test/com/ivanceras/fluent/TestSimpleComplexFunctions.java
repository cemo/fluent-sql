package com.ivanceras.fluent;

import static org.junit.Assert.assertArrayEquals;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static com.ivanceras.fluent.StaticSQL.*;

public class TestSimpleComplexFunctions {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testRecursiveComplexFunctions(){
		String expected =
				" WITH LatestOrders AS (" +
				"		SELECT SUM ( COUNT ( ID ) )," +
				"				COUNT ( MAX ( n_items ) ), " +
				"				CustomerName " +
				"			FROM dbo.Orders" +
				"			RIGHT JOIN Customers" +
				"				on Orders.Customer_ID = Customers.ID " +
				"			LEFT JOIN Persons" +
				"				ON Persons.name = Customer.name" +
				"				AND Persons.lastName = Customer.lastName" +
				"			GROUP BY CustomerID" +
				"		) "+
				" SELECT "+
				"    Customers.*, "+
				"    Orders.OrderTime AS LatestOrderTime, "+
				"    ( SELECT COUNT ( * ) " +
				"		FROM dbo.OrderItems " +
				"		WHERE OrderID IN "+
				"        ( SELECT ID FROM dbo.Orders WHERE CustomerID = Customers.ID ) ) "+
				"            AS TotalItemsPurchased "+
				" FROM dbo.Customers " +
				" INNER JOIN dbo.Orders "+
				"        USING ID" +
				" WHERE "+
				"	Orders.n_items > ? "+
				"   AND Orders.ID IN ( SELECT ID FROM LatestOrders )" ;
		
		Breakdown actual = 
				WITH("LatestOrders", 
					SELECT("CustomerName")
							.SUM(COUNT("ID"))
							.COUNT(MAX("n_items"))
							.FROM("dbo.Orders")
							.RIGHT_JOIN("Customers")
								.ON("Orders.customer_ID", "Customers.ID")
							.LEFT_JOIN("Persons")
								.ON("Persons.name", "Customer.name")
								.AND("Persons.lastName", "Customer.lastName")
							.GROUP_BY("CustomerID")
			)
			.SELECT()
				.FIELD("Customers.*")
				.FIELD("Orders.OrderTime").AS("LatestOrderTime")
				.FIELD(SELECT().COUNT("*")
							.FROM("dbo.OrderItems")
							.WHERE("OrderID").IN(
										SELECT("ID")
										.FROM("dbo.Orders")
										.WHERE("CustomerID").EQUAL_TO_FIELD("Customers.ID"))
							
						).AS("TotalItemsPurchased")
				.FROM("dbo.Customers")
				.INNER_JOIN("dbo.Orders")
					.USING("ID")
				.WHERE("Orders.n_items").GREATER_THAN(0)
				.AND("Orders.ID").IN(SELECT("ID").FROM("LatestOrders"))
			.build();
		System.out.println("expected: \n"+expected);
		System.out.println("actual: \n"+actual.getSql());
		CTest.cassertEquals(expected, actual.getSql());
	}

}
