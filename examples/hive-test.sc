import java.sql.DriverManager

Class.forName("org.apache.hive.jdbc.HiveDriver")

// JDBC连接时，用户名写hadoop，密码不用写
val conn = DriverManager.getConnection("jdbc:hive2://hadoop:10000","","")
val createDatabaseDdl = "create database if not exists db6 "
val stmt = conn.prepareStatement(createDatabaseDdl)
val rs = stmt.executeUpdate()



while (rs.next()) {
  println("product_id:" + rs.getInt("product_id") + " product_name:" + rs.getString("product_name"))
}
rs.close()
stmt.close()
conn.close()
