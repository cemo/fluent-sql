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
      
      


BSD License


link to comments

https://news.ycombinator.com/item?id=5956867
