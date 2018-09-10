package org.postgresql;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.junit.Before;
import org.junit.Test;
import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;

public class VxDriverTest {
  
  VxConnection conn = null;
  
  @Before
  public void setup() throws SQLException {
    String url = "jdbc:postgresql://192.168.3.24:5432/test";
    Properties props = new Properties();
    props.setProperty("PGHOST", "localhost");
    props.setProperty("PGPORT", "5432");
    props.setProperty("user", "postgres");
    props.setProperty("password", "Abcde123");
    conn = VxDriver.makeConnection(url, props);
  }

  @Test
  public void makeConnectionTest() throws SQLException, InterruptedException, ExecutionException {
    assertNotNull(conn);
  }
  
  @Test
  public void queryTest() throws InterruptedException, ExecutionException, SQLException {
    String sql = "select * from test.student";
    
    VxStatement stmt = conn.createStatement();
    
    VxResultSet rs = stmt.executeQuery(sql);
    assertNotNull(rs);
    
    while(rs.next()) {
      String username = rs.getString(1);
      assertEquals("Amy1", username);
    }
    
  }

}
