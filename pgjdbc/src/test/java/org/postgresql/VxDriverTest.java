package org.postgresql;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;
import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.TestUtil;

public class VxDriverTest {
  
  VxConnection conn = null;
  
  @Before
  public void setup() throws SQLException, InterruptedException, ExecutionException {
    String url = "jdbc:postgresql://localhost:5432/test";
    Properties props = new Properties();
    props.setProperty("user", "postgres");
    props.setProperty("password", "Abcde123");
    conn = new VxDriver().connect(url, props).get();
  }

  @Test
  public void makeConnectionTest() throws SQLException, InterruptedException, ExecutionException {
    assertNotNull(conn);
  }
  
  @Test
  public void queryTest() throws InterruptedException, ExecutionException, SQLException {
    String sql = "select name from test.student";
    
    VxStatement stmt = conn.createStatement();
    VxResultSet rs = stmt.executeQuery(sql).get();
    assertNotNull(rs);

    int i = 0;
    while(rs.next().get()) {
      System.out.println(rs.getString(1).get());
      i += 1;
    }

    System.out.println(i);
  }

  @Test
  public void queryTestOriginal() throws Exception {
    Connection conn = TestUtil.openDB();
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("select * from test.student");
    int i = 0;
    while(rs.next()) {
      System.out.println(rs.getString(2));
      i += 1;
    }

    System.out.println(i);
  }

}
