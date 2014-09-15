package com.secondquadrant.jdbcbatchtest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JDBCBatchTest {
	
	Connection conn;
	
	private void doPreparedInsertBatch(int nBatches, boolean useReturning) throws SQLException {
		try {
			String sql = "INSERT INTO prep (a, b) VALUES (?, ?)";
			if (useReturning)
				sql += "RETURNING a, b";
			PreparedStatement ps = conn.prepareStatement(sql);
			for (int i = 0; i < nBatches; i++) {
				ps.setInt(1, i);
				ps.setInt(2, i*100);
				ps.addBatch();
			}
			int[] batchResults = ps.executeBatch();
			for (int i = 0; i < nBatches; i++) {
				if (batchResults[i] != 1)
					throw new java.lang.IllegalStateException("Expected one row affected");
				if (useReturning)
				{
					ResultSet rs = ps.getResultSet();	
					rs.next();
					if (rs.getInt(1) != i || rs.getInt(2) != i*100)
					{
						throw new IllegalStateException("Unexpected values returned");
					}
					rs.close();
					ps.getMoreResults();	 
				}
			}
			ps.close();
		} catch (SQLException ex) {
			ex.getNextException().printStackTrace();
			throw ex;
		}
	}
	
	private void doSetup() throws SQLException {
		Statement st = conn.createStatement();
		st.execute("CREATE TABLE IF NOT EXISTS prep(a integer, b integer)");
		st.close();
	}
	
	private void runTest() throws SQLException {
		try {
			Properties connProps = new Properties();
			connProps.setProperty("user", "craig");
			connProps.setProperty("password", "thinker");
			
			conn = DriverManager.getConnection("jdbc:postgresql://localhost/regress", connProps);

			doSetup();
			doPreparedInsertBatch(100, false);
			// Fails with "A result was returned when none was expected."
			//doPreparedInsertBatch(100, true);
		} finally {
			if (conn != null)
				conn.close();
		}
	}

	public static void main(String[] args) throws SQLException {
		JDBCBatchTest bt = new JDBCBatchTest();
		bt.runTest();
	}

}
