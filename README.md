fluent-sql
==========

A minimalistic Fluent SQL API for Java built with one compilation unit aimed to resemble the code to your original SQL code

Example Usage: 

  A complex query:
  
	   WITH LatestOrders AS
	  ( SELECT MAX (ID)
	   FROM dbo.Orders
	   GROUP BY CustomerID )
	SELECT Customers.*,
	       Orders.OrderTime AS LatestOrderTime,
	
	  (SELECT COUNT (*)
	   FROM dbo.OrderItems
	   WHERE OrderID IN
	       (SELECT ID
	        FROM dbo.Orders
	        WHERE CustomerID = Customers.ID)) AS TotalItemsPurchased
	FROM dbo.Customers
	INNER JOIN dbo.Orders ON Customers.ID = Orders.CustomerID
	WHERE Orders.ID IN
	    (SELECT ID
	     FROM LatestOrders)
     
Using String concatenation:
  
  	String sql = 		" WITH LatestOrders AS (" +
				"		SELECT MAX ( ID ) " +
				"			FROM dbo.Orders " +
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
				"        ON Customers.ID = Orders.CustomerID "+
				" WHERE "+
				"    Orders.ID IN ( SELECT ID FROM LatestOrders )";
  
  
In fluent SQL:
  
    	String actual = new SQL()
			.WITH("LatestOrders", 
					new SQL().SELECT()
							.MAX("ID")
							.FROM("dbo.Orders")
							.GROUP_BY("CustomerID")
			)
			.SELECT()
				.FIELD("Customers.*")
				.FIELD("Orders.OrderTime").AS("LatestOrderTime")
				.FIELD(new SQL().SELECT().COUNT("*")
							.FROM("dbo.OrderItems")
							.WHERE("OrderID").IN(new SQL()
										.SELECT("ID")
										.FROM("dbo.Orders")
										.WHERE("CustomerID").EQUAL_TO_FIELD("Customers.ID"))
							
						).AS("TotalItemsPurchased")
				.FROM("dbo.Customers")
				.INNER_JOIN("dbo.Orders")
					.ON("Customers.ID", "Orders.CustomerID")
				.WHERE("Orders.ID").IN(new SQL()
							.SELECT("ID").FROM("LatestOrders"))
			.build().sql;
      
      

Features
--------------

A SQL breakdown result:
 breakdown.sql (String) - the SQL string
 breakdown.parameters (Object[]) - the resulted array of the parameters that is gathered by the SQL builder.

This will be used in as parameters in your preparedStatment 
	stmt.setObject(i,parameter[i])



BSD License


link to comments

https://news.ycombinator.com/item?id=5956867

Updates from HN suggestions:


Added a static class Helper S.java

		String expected =
				" WITH LatestOrders AS (" +
				"		SELECT SUM ( ( COUNT ( ID ) ) )," +
				"				COUNT ( ( MAX ( n_items ) ) ) , " +
				"				CustomerName," +
				"				( ? ) as color" +
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
				S.WITH("LatestOrders", 
					S.SELECT("CustomerName")
							.SUM(S.COUNT("ID"))
							.COUNT(S.MAX("n_items"))
							.FIELD(S.VALUE("Red")).AS("color")
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
				.FIELD(S.SELECT().COUNT("*")
							.FROM("dbo.OrderItems")
							.WHERE("OrderID").IN(
										S.SELECT("ID")
										.FROM("dbo.Orders")
										.WHERE("CustomerID").EQUAL_TO_FIELD("Customers.ID"))
							
						).AS("TotalItemsPurchased")
				.FROM("dbo.Customers")
				.INNER_JOIN("dbo.Orders")
					.USING("ID")
				.WHERE("Orders.n_items").GREATER_THAN(0)
				.WHERE("Orders.ID").IN(S.SELECT("ID").FROM("LatestOrders"))
			.build();
		System.out.println("expected: \n"+expected);
		System.out.println("actual: \n"+actual.sql);
		CTest.cassertEquals(expected, actual.sql);
		assertArrayEquals(new Object[]{"Red",0}, actual.parameters);
