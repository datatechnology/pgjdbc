/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.postgresql.geometric.PGbox;
import org.postgresql.geometric.PGpoint;
import org.postgresql.jdbc.PgArray;
import org.postgresql.jdbc.PreferQueryMode;
import org.postgresql.jdbc.VxConnection;
import org.postgresql.jdbc.VxPreparedStatement;
import org.postgresql.jdbc.VxResultSet;
import org.postgresql.jdbc.VxStatement;
import org.postgresql.test.VxTestUtil;
import org.postgresql.util.PSQLException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

@RunWith(Parameterized.class)
public class VxArrayTest extends VxBaseTest4 {
  private VxConnection conn;

  public VxArrayTest(BinaryMode binaryMode) {
    setBinaryMode(binaryMode);
  }

  @Parameterized.Parameters(name = "binary = {0}")
  public static Iterable<Object[]> data() {
    Collection<Object[]> ids = new ArrayList<Object[]>();
    for (BinaryMode binaryMode : BinaryMode.values()) {
      ids.add(new Object[]{binaryMode});
    }
    return ids;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    conn = con;
    VxTestUtil.createTable(conn, "arrtest", "intarr int[], decarr decimal(2,1)[], strarr text[]");
  }

  @Override
  public void tearDown() throws SQLException {
    try {
      VxTestUtil.dropTable(conn, "arrtest");
    } catch (InterruptedException | ExecutionException e) {
      throw new SQLException(e);
    }
    super.tearDown();
  }

  @Test
  public void testSetNull() throws SQLException, InterruptedException, ExecutionException {
    VxPreparedStatement pstmt = conn.prepareStatement("INSERT INTO arrtest VALUES (?,?,?)");
    pstmt.setNull(1, Types.ARRAY);
    pstmt.setNull(2, Types.ARRAY);
    pstmt.setNull(3, Types.ARRAY);
    pstmt.executeUpdate().get();

    pstmt.setObject(1, null, Types.ARRAY);
    pstmt.setObject(2, null);
    pstmt.setObject(3, null);
    pstmt.executeUpdate().get();

    pstmt.setArray(1, null);
    pstmt.setArray(2, null);
    pstmt.setArray(3, null);
    pstmt.executeUpdate().get();

    pstmt.close();
  }

  @Test
  public void testSetPrimitiveObjects() throws SQLException, InterruptedException, ExecutionException {
    VxPreparedStatement pstmt = conn.prepareStatement("INSERT INTO arrtest VALUES (?,?,?)");
    pstmt.setObject(1, new int[]{1,2,3}, Types.ARRAY);
    pstmt.setObject(2, new double[]{3.1d, 1.4d}, Types.ARRAY);
    pstmt.setObject(3, new String[]{"abc", "f'a", "fa\"b"}, Types.ARRAY);
    pstmt.executeUpdate().get();
    pstmt.close();

    VxStatement stmt = conn.createStatement();
    VxResultSet rs = stmt.executeQuery("SELECT intarr, decarr, strarr FROM arrtest").get();
    Assert.assertTrue(rs.next().get());

    Array arr = rs.getArray(1).get();
    Assert.assertEquals(Types.INTEGER, arr.getBaseType());
    Integer[] intarr = (Integer[]) arr.getArray();
    assertEquals(3, intarr.length);
    assertEquals(1, intarr[0].intValue());
    assertEquals(2, intarr[1].intValue());
    assertEquals(3, intarr[2].intValue());

    arr = rs.getArray(2).get();
    assertEquals(Types.NUMERIC, arr.getBaseType());
    BigDecimal[] decarr = (BigDecimal[]) arr.getArray();
    assertEquals(2, decarr.length);
    assertEquals(new BigDecimal("3.1"), decarr[0]);
    assertEquals(new BigDecimal("1.4"), decarr[1]);

    arr = rs.getArray(3).get();
    assertEquals(Types.VARCHAR, arr.getBaseType());
    String[] strarr = (String[]) arr.getArray(2, 2);
    assertEquals(2, strarr.length);
    assertEquals("f'a", strarr[0]);
    assertEquals("fa\"b", strarr[1]);

    rs.close();
  }

  @Test
  public void testSetPrimitiveArraysObjects() throws SQLException, InterruptedException, ExecutionException {
    VxPreparedStatement pstmt = conn.prepareStatement("INSERT INTO arrtest VALUES (?,?,?)");

    final VxConnection arraySupport = conn.unwrap(VxConnection.class);

    pstmt.setArray(1, arraySupport.createArrayOf("int4", new int[] { 1, 2, 3 }));
    pstmt.setObject(2, arraySupport.createArrayOf("float8", new double[] { 3.1d, 1.4d }));
    pstmt.setObject(3, arraySupport.createArrayOf("varchar", new String[] { "abc", "f'a", "fa\"b" }));

    pstmt.executeUpdate().get();
    pstmt.close();

    VxStatement stmt = conn.createStatement();
    VxResultSet rs = stmt.executeQuery("SELECT intarr, decarr, strarr FROM arrtest").get();
    Assert.assertTrue(rs.next().get());

    Array arr = rs.getArray(1).get();
    Assert.assertEquals(Types.INTEGER, arr.getBaseType());
    Integer[] intarr = (Integer[]) arr.getArray();
    Assert.assertEquals(3, intarr.length);
    Assert.assertEquals(1, intarr[0].intValue());
    Assert.assertEquals(2, intarr[1].intValue());
    Assert.assertEquals(3, intarr[2].intValue());

    arr = rs.getArray(2).get();
    Assert.assertEquals(Types.NUMERIC, arr.getBaseType());
    BigDecimal[] decarr = (BigDecimal[]) arr.getArray();
    Assert.assertEquals(2, decarr.length);
    Assert.assertEquals(new BigDecimal("3.1"), decarr[0]);
    Assert.assertEquals(new BigDecimal("1.4"), decarr[1]);

    arr = rs.getArray(3).get();
    Assert.assertEquals(Types.VARCHAR, arr.getBaseType());
    String[] strarr = (String[]) arr.getArray(2, 2);
    Assert.assertEquals(2, strarr.length);
    Assert.assertEquals("f'a", strarr[0]);
    Assert.assertEquals("fa\"b", strarr[1]);

    try {
      arraySupport.createArrayOf("int4", Integer.valueOf(1));
      fail("not an array");
    } catch (PSQLException e) {

    }

    rs.close();
  }

  @Test
  public void testSetNullArrays() throws SQLException, InterruptedException, ExecutionException {
    VxPreparedStatement pstmt = conn.prepareStatement("INSERT INTO arrtest VALUES (?,?,?)");

    final VxConnection arraySupport = conn.unwrap(VxConnection.class);
	pstmt.setArray(1, arraySupport.createArrayOf("int4", null));
    pstmt.setObject(2, conn.createArrayOf("float8", null));
    pstmt.setObject(3, arraySupport.createArrayOf("varchar", null));

    pstmt.executeUpdate().get();
    pstmt.close();

    VxStatement stmt = conn.createStatement();
    VxResultSet rs = stmt.executeQuery("SELECT intarr, decarr, strarr FROM arrtest").get();
    Assert.assertTrue(rs.next().get());

    Array arr = rs.getArray(1).get();
    Assert.assertNull(arr);

    arr = rs.getArray(2).get();
    Assert.assertNull(arr);

    arr = rs.getArray(3).get();
    Assert.assertNull(arr);

    rs.close();
  }

  @Test
  public void testRetrieveArrays() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = conn.createStatement();

    // you need a lot of backslashes to get a double quote in.
    stmt.executeUpdate("INSERT INTO arrtest VALUES ('{1,2,3}','{3.1,1.4}', '"
        + VxTestUtil.escapeString(conn, "{abc,f'a,\"fa\\\"b\",def}") + "')").get();

    VxResultSet rs = stmt.executeQuery("SELECT intarr, decarr, strarr FROM arrtest").get();
    Assert.assertTrue(rs.next().get());

    Array arr = rs.getArray(1).get();
    Assert.assertEquals(Types.INTEGER, arr.getBaseType());
    Integer[] intarr = (Integer[]) arr.getArray();
    Assert.assertEquals(3, intarr.length);
    Assert.assertEquals(1, intarr[0].intValue());
    Assert.assertEquals(2, intarr[1].intValue());
    Assert.assertEquals(3, intarr[2].intValue());

    arr = rs.getArray(2).get();
    Assert.assertEquals(Types.NUMERIC, arr.getBaseType());
    BigDecimal[] decarr = (BigDecimal[]) arr.getArray();
    Assert.assertEquals(2, decarr.length);
    Assert.assertEquals(new BigDecimal("3.1"), decarr[0]);
    Assert.assertEquals(new BigDecimal("1.4"), decarr[1]);

    arr = rs.getArray(3).get();
    Assert.assertEquals(Types.VARCHAR, arr.getBaseType());
    String[] strarr = (String[]) arr.getArray(2, 2);
    Assert.assertEquals(2, strarr.length);
    Assert.assertEquals("f'a", strarr[0]);
    Assert.assertEquals("fa\"b", strarr[1]);

    rs.close();
    stmt.close();
  }

  @Test
  public void testRetrieveResultSets() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = conn.createStatement();

    // you need a lot of backslashes to get a double quote in.
    stmt.executeUpdate("INSERT INTO arrtest VALUES ('{1,2,3}','{3.1,1.4}', '"
        + VxTestUtil.escapeString(conn, "{abc,f'a,\"fa\\\"b\",def}") + "')").get();

    VxResultSet rs = stmt.executeQuery("SELECT intarr, decarr, strarr FROM arrtest").get();
    Assert.assertTrue(rs.next().get());

    Array arr = rs.getArray(1).get();
    Assert.assertEquals(Types.INTEGER, arr.getBaseType());
    java.sql.ResultSet arrrs = arr.getResultSet();
    Assert.assertTrue(arrrs.next());
    Assert.assertEquals(1, arrrs.getInt(1));
    Assert.assertEquals(1, arrrs.getInt(2));
    Assert.assertTrue(arrrs.next());
    Assert.assertEquals(2, arrrs.getInt(1));
    Assert.assertEquals(2, arrrs.getInt(2));
    Assert.assertTrue(arrrs.next());
    Assert.assertEquals(3, arrrs.getInt(1));
    Assert.assertEquals(3, arrrs.getInt(2));
    Assert.assertTrue(!arrrs.next());
    Assert.assertTrue(arrrs.previous());
    Assert.assertEquals(3, arrrs.getInt(2));
    arrrs.first();
    Assert.assertEquals(1, arrrs.getInt(2));
    arrrs.close();

    arr = rs.getArray(2).get();
    Assert.assertEquals(Types.NUMERIC, arr.getBaseType());
    arrrs = arr.getResultSet();
    Assert.assertTrue(arrrs.next());
    Assert.assertEquals(new BigDecimal("3.1"), arrrs.getBigDecimal(2));
    Assert.assertTrue(arrrs.next());
    Assert.assertEquals(new BigDecimal("1.4"), arrrs.getBigDecimal(2));
    arrrs.close();

    arr = rs.getArray(3).get();
    Assert.assertEquals(Types.VARCHAR, arr.getBaseType());
    arrrs = arr.getResultSet(2, 2);
    Assert.assertTrue(arrrs.next());
    Assert.assertEquals(2, arrrs.getInt(1));
    Assert.assertEquals("f'a", arrrs.getString(2));
    Assert.assertTrue(arrrs.next());
    Assert.assertEquals(3, arrrs.getInt(1));
    Assert.assertEquals("fa\"b", arrrs.getString(2));
    Assert.assertTrue(!arrrs.next());
    arrrs.close();

    rs.close();
    stmt.close();
  }

  @Test
  public void testSetArray() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = conn.createStatement();
    VxResultSet arrRS = stmt.executeQuery("SELECT '{1,2,3}'::int4[]").get();
    Assert.assertTrue(arrRS.next().get());
    Array arr = arrRS.getArray(1).get();
    arrRS.close();
    stmt.close();

    VxPreparedStatement pstmt = conn.prepareStatement("INSERT INTO arrtest(intarr) VALUES (?)");
    pstmt.setArray(1, arr);
    pstmt.executeUpdate().get();

    pstmt.setObject(1, arr, Types.ARRAY);
    pstmt.executeUpdate().get();

    pstmt.setObject(1, arr);
    pstmt.executeUpdate().get();

    pstmt.close();

    VxStatement select = conn.createStatement();
    VxResultSet rs = select.executeQuery("SELECT intarr FROM arrtest").get();
    int resultCount = 0;
    while (rs.next().get()) {
      resultCount++;
      Array result = rs.getArray(1).get();
      Assert.assertEquals(Types.INTEGER, result.getBaseType());
      Assert.assertEquals("int4", result.getBaseTypeName());

      Integer[] intarr = (Integer[]) result.getArray();
      Assert.assertEquals(3, intarr.length);
      Assert.assertEquals(1, intarr[0].intValue());
      Assert.assertEquals(2, intarr[1].intValue());
      Assert.assertEquals(3, intarr[2].intValue());
    }
    Assert.assertEquals(3, resultCount);
  }


  /**
   * Starting with 8.0 non-standard (beginning index isn't 1) bounds the dimensions are returned in
   * the data. The following should return "[0:3]={0,1,2,3,4}" when queried. Older versions simply
   * do not return the bounds.
   * @throws ExecutionException 
   * @throws InterruptedException 
   */
  @Test
  public void testNonStandardBounds() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = conn.createStatement();
    stmt.executeUpdate("INSERT INTO arrtest (intarr) VALUES ('{1,2,3}')").get();
    stmt.executeUpdate("UPDATE arrtest SET intarr[0] = 0").get();
    VxResultSet rs = stmt.executeQuery("SELECT intarr FROM arrtest").get();
    Assert.assertTrue(rs.next().get());
    Array result = rs.getArray(1).get();
    Integer[] intarr = (Integer[]) result.getArray();
    Assert.assertEquals(4, intarr.length);
    for (int i = 0; i < intarr.length; i++) {
      Assert.assertEquals(i, intarr[i].intValue());
    }
  }

  @Test
  public void testMultiDimensionalArray() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = conn.createStatement();
    VxResultSet rs = stmt.executeQuery("SELECT '{{1,2},{3,4}}'::int[]").get();
    Assert.assertTrue(rs.next().get());
    Array arr = rs.getArray(1).get();
    Object[] oa = (Object[]) arr.getArray();
    Assert.assertEquals(2, oa.length);
    Integer[] i0 = (Integer[]) oa[0];
    Assert.assertEquals(2, i0.length);
    Assert.assertEquals(1, i0[0].intValue());
    Assert.assertEquals(2, i0[1].intValue());
    Integer[] i1 = (Integer[]) oa[1];
    Assert.assertEquals(2, i1.length);
    Assert.assertEquals(3, i1[0].intValue());
    Assert.assertEquals(4, i1[1].intValue());
    rs.close();
    stmt.close();
  }

  @Test
  public void testNullValues() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = conn.createStatement();
    VxResultSet rs = stmt.executeQuery("SELECT ARRAY[1,NULL,3]").get();
    Assert.assertTrue(rs.next().get());
    Array arr = rs.getArray(1).get();
    Integer[] i = (Integer[]) arr.getArray();
    Assert.assertEquals(3, i.length);
    Assert.assertEquals(1, i[0].intValue());
    Assert.assertNull(i[1]);
    Assert.assertEquals(3, i[2].intValue());
  }

  @Test
  public void testNullFieldString() throws SQLException {
    Array arr = new PgArray(conn.createConnection(), 1, (String) null);
    Assert.assertNull(arr.toString());
  }

  @Test
  public void testUnknownArrayType() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = conn.createStatement();
    VxResultSet rs =
        stmt.executeQuery("SELECT relacl FROM pg_class WHERE relacl IS NOT NULL LIMIT 1").get();
    ResultSetMetaData rsmd = rs.getMetaData();
    Assert.assertEquals(Types.ARRAY, rsmd.getColumnType(1));

    Assert.assertTrue(rs.next().get());
    Array arr = rs.getArray(1).get();
    Assert.assertEquals("aclitem", arr.getBaseTypeName());

    java.sql.ResultSet arrRS = arr.getResultSet();
    ResultSetMetaData arrRSMD = arrRS.getMetaData();
    Assert.assertEquals("aclitem", arrRSMD.getColumnTypeName(2));
  }

  @Test
  public void testRecursiveResultSets() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = conn.createStatement();
    VxResultSet rs = stmt.executeQuery("SELECT '{{1,2},{3,4}}'::int[]").get();
    Assert.assertTrue(rs.next().get());
    Array arr = rs.getArray(1).get();

    java.sql.ResultSet arrRS = arr.getResultSet();
    ResultSetMetaData arrRSMD = arrRS.getMetaData();
    Assert.assertEquals(Types.ARRAY, arrRSMD.getColumnType(2));
    Assert.assertEquals("_int4", arrRSMD.getColumnTypeName(2));

    Assert.assertTrue(arrRS.next());
    Assert.assertEquals(1, arrRS.getInt(1));
    Array a1 = arrRS.getArray(2);
    java.sql.ResultSet a1RS = a1.getResultSet();
    ResultSetMetaData a1RSMD = a1RS.getMetaData();
    Assert.assertEquals(Types.INTEGER, a1RSMD.getColumnType(2));
    Assert.assertEquals("int4", a1RSMD.getColumnTypeName(2));

    Assert.assertTrue(a1RS.next());
    Assert.assertEquals(1, a1RS.getInt(2));
    Assert.assertTrue(a1RS.next());
    Assert.assertEquals(2, a1RS.getInt(2));
    Assert.assertTrue(!a1RS.next());
    a1RS.close();

    Assert.assertTrue(arrRS.next());
    Assert.assertEquals(2, arrRS.getInt(1));
    Array a2 = arrRS.getArray(2);
    java.sql.ResultSet a2RS = a2.getResultSet();

    Assert.assertTrue(a2RS.next());
    Assert.assertEquals(3, a2RS.getInt(2));
    Assert.assertTrue(a2RS.next());
    Assert.assertEquals(4, a2RS.getInt(2));
    Assert.assertTrue(!a2RS.next());
    a2RS.close();

    arrRS.close();
    rs.close();
    stmt.close();
  }

  @Test
  public void testNullString() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = conn.createStatement();
    VxResultSet rs = stmt.executeQuery("SELECT '{a,NULL}'::text[]").get();
    Assert.assertTrue(rs.next().get());
    Array arr = rs.getArray(1).get();

    String[] s = (String[]) arr.getArray();
    Assert.assertEquals(2, s.length);
    Assert.assertEquals("a", s[0]);
    Assert.assertNull(s[1]);
  }

  @Test
  public void testEscaping() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = conn.createStatement();
    String sql = "SELECT ";
    sql += 'E';
    // Uggg. Three levels of escaping: Java, string literal, array.
    sql += "'{{c\\\\\"d, ''}, {\"\\\\\\\\\",\"''\"}}'::text[]";

    VxResultSet rs = stmt.executeQuery(sql).get();
    Assert.assertTrue(rs.next().get());

    Array arr = rs.getArray(1).get();
    String[][] s = (String[][]) arr.getArray();
    Assert.assertEquals("c\"d", s[0][0]);
    Assert.assertEquals("'", s[0][1]);
    Assert.assertEquals("\\", s[1][0]);
    Assert.assertEquals("'", s[1][1]);

    java.sql.ResultSet arrRS = arr.getResultSet();

    Assert.assertTrue(arrRS.next());
    Array a1 = arrRS.getArray(2);
    java.sql.ResultSet rs1 = a1.getResultSet();
    Assert.assertTrue(rs1.next());
    Assert.assertEquals("c\"d", rs1.getString(2));
    Assert.assertTrue(rs1.next());
    Assert.assertEquals("'", rs1.getString(2));
    Assert.assertTrue(!rs1.next());

    Assert.assertTrue(arrRS.next());
    Array a2 = arrRS.getArray(2);
    java.sql.ResultSet rs2 = a2.getResultSet();
    Assert.assertTrue(rs2.next());
    Assert.assertEquals("\\", rs2.getString(2));
    Assert.assertTrue(rs2.next());
    Assert.assertEquals("'", rs2.getString(2));
    Assert.assertTrue(!rs2.next());
  }

  @Test
  public void testWriteMultiDimensional() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = conn.createStatement();
    VxResultSet rs = stmt.executeQuery("SELECT '{{1,2},{3,4}}'::int[]").get();
    Assert.assertTrue(rs.next().get());
    Array arr = rs.getArray(1).get();
    rs.close();
    stmt.close();

    String sql = "SELECT ?";
    if (preferQueryMode == PreferQueryMode.SIMPLE) {
      sql = "SELECT ?::int[]";
    }
    VxPreparedStatement pstmt = conn.prepareStatement(sql);
    pstmt.setArray(1, arr);
    rs = pstmt.executeQuery().get();
    Assert.assertTrue(rs.next().get());
    arr = rs.getArray(1).get();

    Integer[][] i = (Integer[][]) arr.getArray();
    Assert.assertEquals(1, i[0][0].intValue());
    Assert.assertEquals(2, i[0][1].intValue());
    Assert.assertEquals(3, i[1][0].intValue());
    Assert.assertEquals(4, i[1][1].intValue());
  }

  /*
   * The box data type uses a semicolon as the array element delimiter instead of a comma which
   * pretty much everything else uses.
   */
  @Test
  public void testNonStandardDelimiter() throws SQLException, InterruptedException, ExecutionException {
    VxStatement stmt = conn.createStatement();
    VxResultSet rs = stmt.executeQuery("SELECT '{(3,4),(1,2);(7,8),(5,6)}'::box[]").get();
    Assert.assertTrue(rs.next().get());
    Array arr = rs.getArray(1).get();

    java.sql.ResultSet arrRS = arr.getResultSet();

    Assert.assertTrue(arrRS.next());
    PGbox box1 = (PGbox) arrRS.getObject(2);
    PGpoint p1 = box1.point[0];
    Assert.assertEquals(3, p1.x, 0.001);
    Assert.assertEquals(4, p1.y, 0.001);

    Assert.assertTrue(arrRS.next());
    PGbox box2 = (PGbox) arrRS.getObject(2);
    PGpoint p2 = box2.point[1];
    Assert.assertEquals(5, p2.x, 0.001);
    Assert.assertEquals(6, p2.y, 0.001);

    Assert.assertTrue(!arrRS.next());
  }

  @Test
  public void testEmptyArray() throws SQLException, InterruptedException, ExecutionException {
    VxPreparedStatement pstmt = conn.prepareStatement("SELECT '{}'::int[]");
    VxResultSet rs = pstmt.executeQuery().get();

    while (rs.next().get()) {
      Array array = rs.getArray(1).get();
      if (!rs.wasNull()) {
        java.sql.ResultSet ars = array.getResultSet();
        Assert.assertEquals("get columntype should return Types.INTEGER", java.sql.Types.INTEGER,
            ars.getMetaData().getColumnType(1));
      }
    }
  }

}

