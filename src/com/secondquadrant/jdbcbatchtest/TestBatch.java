package com.secondquadrant.jdbcbatchtest;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.util.PSQLException;

public class TestBatch {

	static Connection conn;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Properties connProps = new Properties();
		connProps.setProperty("user", "craig");
		connProps.setProperty("password", "thinker");
		
		conn = DriverManager.getConnection("jdbc:postgresql://localhost/regress", connProps);
		
		Statement st = conn.createStatement();
		st.execute("DROP TABLE IF EXISTS prep");
		st.execute("CREATE TABLE prep(a integer, b integer)");
		st.execute("DROP TABLE IF EXISTS bigbatch;");
		st.execute("CREATE TABLE bigbatch(id serial primary key, blather text);");
		st.close();
		
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		conn.close();
	}

	@Test @Ignore
	public void testPreparedInsertBatchReturning() throws SQLException {
		// Fails with "result ... where no result expected"
		preparedInsertBatchCommon(100, true);
	}	
	
	@Test
	public void testPreparedInsertBatch() throws SQLException {
		preparedInsertBatchCommon(100, false);
	}
	
	private void preparedInsertBatchCommon(int nBatches, boolean useReturning) throws SQLException {		
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
				assertEquals("Expected one row affected", 1, batchResults[i]);
				if (useReturning)
				{
					ResultSet rs = ps.getResultSet();	
					rs.next();
					assertEquals(i, rs.getInt(1));
					assertEquals(i*100, rs.getInt(2));
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
	
	@Test
	public void testBigPrepareBatch() throws SQLException
	{
		String bigstring = StringUtils.repeat("this is a pile of blather", 100);
				
		PreparedStatement ps = conn.prepareStatement("INSERT INTO bigbatch(blather) VALUES (?)");
		for (int i = 0; i < 100; i++)
		{
			ps.setString(1, bigstring);
			ps.addBatch();
		}
		ps.executeBatch();		
	}
	
	@Test
	public void testMixedBatch() throws SQLException {
		try {
			Statement st = conn.createStatement();
			st.addBatch("INSERT INTO prep (a, b) VALUES (1,2)");
			st.addBatch("INSERT INTO prep (a, b) VALUES (100,200)");
			st.addBatch("DELETE FROM prep WHERE a = 1 AND b = 2");
			st.addBatch("CREATE TEMPORARY TABLE waffles(sauce text)");
			st.addBatch("INSERT INTO waffles(sauce) VALUES ('cream'), ('strawberry jam')");
			int[] batchResult = st.executeBatch();
			assertEquals(1, batchResult[0]);
			assertEquals(1, batchResult[1]);
			assertEquals(3, batchResult[2]);
			assertEquals(0, batchResult[3]);
			assertEquals(2, batchResult[4]);
		} catch (SQLException ex) {
			ex.getNextException().printStackTrace();
			throw ex;
		}
	}
	
	@Test
	public void testPreparedBatchReturning() throws SQLException {
		try {
			PreparedStatement st = conn.prepareStatement("INSERT INTO prep(a,b) VALUES (?,?) RETURNING a");
			st.setInt(1, 1);
			st.setInt(2, 2);
			st.addBatch();
			st.executeBatch();
			fail("Must throw");
		} catch (SQLException ex) {
			assertTrue("Expected PSQLException", ex.getNextException() instanceof PSQLException);
			assertEquals("Expected SQLState", "0100E", ex.getSQLState());
		}
	}
	
	@Test
	public void testStatementBatchReturning() throws SQLException {
		try {
			Statement st = conn.createStatement();
			st.addBatch("INSERT INTO prep(a,b) VALUES (1,2) RETURNING a");
			st.executeBatch();
			fail("Must throw");
		} catch (SQLException ex) {
			assertTrue("Expected PSQLException", ex.getNextException() instanceof PSQLException);
			assertEquals("Expected SQLState", "0100E", ex.getSQLState());
		}
	}

	@Test
	public void testPreparedGeneratedKeysReturning() throws SQLException {
		try {
			PreparedStatement st = conn.prepareStatement("INSERT INTO prep(a,b) VALUES (?,?) RETURNING a");
			st.setInt(1, 1);
			st.setInt(2, 2);
			st.executeUpdate();
			fail("Must throw");
		} catch (SQLException ex) {
			assertTrue("Expected PSQLException", ex instanceof PSQLException);
			assertEquals("Expected SQLState", "0100E", ex.getSQLState());
		}
	}
	
	@Test
	public void testPreparedGeneratedKeys() throws SQLException {
		PreparedStatement st = conn.prepareStatement("INSERT INTO prep(a,b) VALUES (?,?)");
		st.setInt(1, 1);
		st.setInt(2, 2);
		st.executeUpdate();
		ResultSet x = st.getGeneratedKeys();
		// No generated keys are returned
		assertFalse("Should not get any result set", x.next());
		// This is what we'd like, but not what actually happens:
		//assertTrue("Must have one result", x.next());
		//assertEquals(1, x.getInt(1));
		//assertEquals(2, x.getInt(2));
	}
	
	@Test
	public void testStatementExecuteReturningGeneratedKeys() throws SQLException {
		Statement st = conn.createStatement();
		ResultSet rs;
		st.execute("INSERT INTO prep(a,b) VALUES (1,2)", Statement.RETURN_GENERATED_KEYS);
		assertNull("getResultSet() must return null", st.getResultSet());
		rs = st.getGeneratedKeys();
		assertTrue("Expected result set", rs.next());
		assertEquals("Should return all cols", 1, rs.getInt(1));
		assertEquals("Should return all cols", 2, rs.getInt(2));
		assertFalse("Expected no more results", rs.next());
	}
	
	@Test
	public void testStatementExecuteReturningGeneratedKeysColNames() throws SQLException {
		Statement st = conn.createStatement();
		ResultSet rs;
		st.execute("INSERT INTO prep(a,b) VALUES (1,2)",
				new String[] {"a"});
		assertNull("getResultSet() must return null", st.getResultSet());
		rs = st.getGeneratedKeys();
		assertTrue("Expected result set", rs.next());
		assertEquals("Should return all cols", 1, rs.getInt(1));
		try {
			rs.getInt(2);
			fail("Must not have a second column");
		} catch (PSQLException ex) {
			assertEquals("22023", ex.getSQLState());
		}
		assertFalse("Expected no more results", rs.next());
	}
	
	@Test
	public void testStatementExecuteReturningGeneratedKeysColIndexes() throws SQLException {
		Statement st = conn.createStatement();
		try {
			st.execute("INSERT INTO prep(a,b) VALUES (1,2)",
					new int[] {1});
			fail("PgJDBC doesn't support returning column indexes for generated keys");
		} catch (PSQLException ex) {
			assertEquals("0A000", ex.getSQLState());
		}
	}
	
	@Test
	public void testPreparedBatchResultSet() throws SQLException {
	    PreparedStatement s = conn.prepareStatement(
	        "INSERT INTO prep(a,b) VALUES (?, ?)",
	        new String[]{"a"});
	
	    for (int i = 0; i < 10; i++) {
	        s.setInt(1, i);
	        s.setInt(2, i);
	        s.addBatch();
	    }
	    
	    s.executeBatch();
	    
	    ResultSet rs;
	    
	    rs = s.getGeneratedKeys();
	    
	    for (int i = 0; i < 10; i++) {
		    assertTrue("Must have generated keys", rs.next());
		    assertEquals("Key must equal inserted value", i, rs.getInt(1));
	    }
	    assertFalse(rs.next());
	}

}
