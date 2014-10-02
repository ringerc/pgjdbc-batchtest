package com.secondquadrant.jdbcbatchtest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/* 
 * Hack this to add any connection properties required for your
 * environment. Or just create a 'regress' DB owned by
 * your current user with a 'pg_hba.conf' that uses peer/trust
 * for it.
 */
public class JDBCConnectionFactory {

	static Connection getConnection(Properties connProps) throws SQLException {
		// Add your properties here...
		return DriverManager.getConnection("jdbc:postgresql://localhost/regress", connProps);
	}
	
	static Connection getConnection() throws SQLException {
		Properties connProps = new Properties();
		return JDBCConnectionFactory.getConnection(connProps);
	}
}
