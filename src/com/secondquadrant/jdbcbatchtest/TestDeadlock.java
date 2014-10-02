package com.secondquadrant.jdbcbatchtest;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDeadlock {
	
	private static Connection conn;
	
	/* 
	 * Enough to massively over-fill any sane buffers.
	 * 
	 * A deadlock can arise without this level of abuse, but this makes it
	 * easy to reproduce on a fast, high bandwidth loopback interface with big
	 * buffers.
	 */
	private static final int nRepeats = 131072,
							 nQueries = 32;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Properties connProps = new Properties();
		connProps.setProperty("user", System.getProperty("user.name"));
		connProps.setProperty("password", System.getProperty("jdbcPassword"));
		connProps.setProperty("loglevel", "2");
		
		conn = DriverManager.getConnection("jdbc:postgresql://localhost/regress", connProps);

		Statement st = conn.createStatement();
		
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
						+"    largetext varchar(65536) not null"
						+");");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test(timeout=60*1000)
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
		final String padding64k = StringUtils.repeat("deadbeef", nRepeats);
		try {
			PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO deadlock_demo1(id, largetext) VALUES (?,?)");
			/* 9 loops should be enough, but keep going ... */
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

}