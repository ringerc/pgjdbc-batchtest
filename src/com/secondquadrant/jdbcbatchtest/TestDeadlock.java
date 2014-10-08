package com.secondquadrant.jdbcbatchtest;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.net.Socket;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.core.PGStream;
import org.postgresql.core.ProtocolConnection;
import org.postgresql.jdbc2.AbstractJdbc2Connection;

/*
 * A test to 
 */
public class TestDeadlock {
	
	private Connection conn;
	
	/* 
	 * Enough to massively over-fill any sane buffers.
	 * 
	 * A deadlock can arise without this level of abuse, but this makes it
	 * easy to reproduce on a fast, high bandwidth loopback interface with big
	 * buffers.
	 */
	//private static final int nRepeats = 131072,
	//						 nQueries = 32;
	private static final int nRepeats = 1,
							 nQueries = 2000;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Connection setupConn = JDBCConnectionFactory.getConnection();

		Statement st = setupConn.createStatement();
		
		/* For batch without keys */
		st.executeUpdate("DROP TABLE IF EXISTS deadlock_demo1;");
		
		st.executeUpdate("CREATE TABLE deadlock_demo1("
						+"    id integer primary key,"
						+"    largetext varchar not null"
						+");");
		
		st.executeUpdate("CREATE OR REPLACE FUNCTION bignoisetrigger() RETURNS trigger "
						+"LANGUAGE plpgsql AS $$\n"
						+"BEGIN\n"
						+"	RAISE WARNING '%',repeat('abcdefgh', " + nRepeats + ");\n"
						+"  RETURN NULL;\n"
						+"END;\n"
						+"$$;");
		
		st.executeUpdate("CREATE TRIGGER bignoise\n"
						+"AFTER INSERT ON deadlock_demo1\n"
						+"FOR EACH ROW EXECUTE PROCEDURE bignoisetrigger();");
		

		/* For the batch with keys case */
		st.executeUpdate("DROP TABLE IF EXISTS deadlock_demo2;");
		
		st.executeUpdate("CREATE TABLE deadlock_demo2("
						+"    id integer primary key,"
						+"    largetext varchar(2097152) not null"
						+");");
		
		setupConn.close();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}
	
	@Before
	public void before() throws Exception {
		Properties props = new Properties();
		//props.setProperty("loglevel","2");
		props.setProperty("prepareThreshold", "1");
		
		conn = JDBCConnectionFactory.getConnection(props);
	}
	
	@After
	public void after() throws Exception {
		try {
			conn.close();
		} catch (SQLException e) {
			System.err.println("During after(): ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testBufferSize() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, SocketException {
		// Discover the socket buffer size by poking in the driver guts
		assertNotNull(conn);
		
		// We must use AbstractJdbc2Connection directly, as that's the declaring class
		Field pgProtoConnField = AbstractJdbc2Connection.class.getDeclaredField("protoConnection");
		pgProtoConnField.setAccessible(true);
		
		ProtocolConnection pc = (ProtocolConnection)pgProtoConnField.get(conn);
		Field pgstreamField = pc.getClass().getDeclaredField("pgStream");
		pgstreamField.setAccessible(true);
		
		PGStream pgs = (PGStream) pgstreamField.get(pc);
		Socket s = pgs.getSocket();
		System.err.println("PgJDBC send buffer size is: " + s.getSendBufferSize());
	}

	private void printBackendPid(String testName) throws SQLException {
		Statement st = conn.createStatement();
		st.execute("SELECT pg_backend_pid()");
		ResultSet rs = st.getResultSet();
		assertTrue(rs.next());		
		System.out.println("PostgreSQL backend pid for "+testName+"() is " + rs.getInt(1));
		st.close();
	}
	
	@Test(timeout=15*1000) @Ignore
	public void testBatchDeadlock() throws SQLException {
		/* 
		 * Demonstrate that a client/server deadlock can still occur without RETURNING
		 * if we have async notifications or log messages on a table.
		 * 
		 * The JDBC driver expects that replies will be no bigger than 250 bytes per message,
		 * and that the server will have a 64k send buffer.
		 * 
		 * In practice, most modern Linux systems default to over 200kb send and recv buffers
		 * so we'll need to send 400k of data to create a deadlock so we'll use a trigger that
		 * spews big chunks of WARNINGs, many kb per WARNING. See the test setup code.
		 * 
		 * We also have to be able to fill our send buffer and the server's receive buffer,
		 * so we need to send about the same amount of data in each request.
		 */
		printBackendPid("testBatchDeadlock");
		
		final String padding64k = StringUtils.repeat("deadbeef", nRepeats);
		try {
			PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO deadlock_demo1(id, largetext) VALUES (?,?)");

			for (int i = 0; i < nQueries; i++) {
				ps.setInt(1, i);
				ps.setString(2, padding64k);
				ps.addBatch();
			}
			int[] batchresult = ps.executeBatch();
			assertEquals("Batch must contain expected result count", nQueries, batchresult.length);
			int[] expectedresult = new int[nQueries];
			Arrays.fill(expectedresult, 1);
			assertArrayEquals("Each batch item must succeed", expectedresult, batchresult);
		} catch (SQLException ex) {
			ex.getNextException().printStackTrace();
			throw ex;
		}
	}
	
	/* 
	 * Why does this hit DESCRIBE once per batch entry? The results should/must be the same.

		QueryExecutorImpl.sendDescribePortal(SimpleQuery, Portal) line: 1427	
		QueryExecutorImpl.sendOneQuery(SimpleQuery, SimpleParameterList, int, int, int) line: 1631	
		QueryExecutorImpl.sendQuery(V3Query, V3ParameterList, int, int, int, QueryExecutorImpl$ErrorTrackingResultHandler) line: 1137	
		QueryExecutorImpl.execute(Query[], ParameterList[], ResultHandler, int, int, int) line: 396	
		Jdbc4PreparedStatement(AbstractJdbc2Statement).executeBatch() line: 2897	
		TestDeadlock.testBatchDeadlock() line: 142	
		
		Answer: https://github.com/pgjdbc/pgjdbc/issues/196

	*/
	
	@Test(timeout=15*1000) @Ignore
	public void testBatchDeadlockReturning() throws SQLException {
		/* 
		 * Test client/server deadlock with RETURNING clause
		 */
		printBackendPid("testBatchDeadlockReturning");
		
		final String padding64k = StringUtils.repeat("deadbeef", nRepeats);
		try {
			/* 
			 * Note that there's no trigger to notices on deadlock_demo2,
			 * instead the returned text value from the RETURNING clause 
			 * will have the same effect.
			 */
			PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO deadlock_demo2(id, largetext) VALUES (?,?)",
					new String[] { "id", "largetext" }
					);
			for (int i = 0; i < nQueries; i++) {
				ps.setInt(1, i);
				ps.setString(2, padding64k);
				ps.addBatch();
			}
			int[] batchresult = ps.executeBatch();
			assertEquals("Batch must contain expected result count", nQueries, batchresult.length);
			int[] expectedresult = new int[nQueries];
			Arrays.fill(expectedresult, 1);
			assertArrayEquals("Each batch item must succeed", expectedresult, batchresult);
			
			 ResultSet rs = ps.getGeneratedKeys();

		    for (int i = 0; i < nQueries; i++) {
		        assertTrue("Must have generated keys", rs.next());
		        assertEquals("Result must equal inserted value",
		                          i, rs.getInt(1));
		        assertEquals("Fetched padding data must match",
		        				  padding64k, rs.getString(2));
		    }
		    assertFalse(rs.next());
		} catch (SQLException ex) {
			ex.getNextException().printStackTrace();
			throw ex;
		}
	}

	/* 
	 * Now run much the same test yet again, but this time only fetch small keys.
	 * 
	 * It shouldn't deadlock, and should run a true batch.
	 */
	@Test
	public void testBatchSmallReturning() throws SQLException {
		/* 
		 * Test client/server deadlock with RETURNING clause
		 */
		printBackendPid("testBatchDeadlockReturning");
		
		final String padding64k = StringUtils.repeat("deadbeef", nRepeats);
		try {
			/* 
			 * Here we fetch just a small value as the return; batching
			 * should be enabled even though we're sending big values.
			 */
			PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO deadlock_demo2(id, largetext) VALUES (?,?)",
					new String[] { "id" }
					);
			for (int i = 0; i < nQueries; i++) {
				ps.setInt(1, i);
				ps.setString(2, padding64k);
				ps.addBatch();
			}
			int[] batchresult = ps.executeBatch();
			assertEquals("Batch must contain expected result count", nQueries, batchresult.length);
			int[] expectedresult = new int[nQueries];
			Arrays.fill(expectedresult, 1);
			assertArrayEquals("Each batch item must succeed", expectedresult, batchresult);
			
			 ResultSet rs = ps.getGeneratedKeys();

		    for (int i = 0; i < nQueries; i++) {
		        assertTrue("Must have generated keys", rs.next());
		        assertEquals("Result must equal inserted value",
		                          i, rs.getInt(1));
		    }
		    assertFalse(rs.next());
		} catch (SQLException ex) {
			ex.getNextException().printStackTrace();
			throw ex;
		}
	}
}
