package org.postgresql.jdbc;


public class VxResultWrapper {

  public VxResultWrapper(VxResultSet rs) {
    this.rs = rs;
    this.updateCount = -1;
    this.insertOID = -1;
  }

  public VxResultWrapper(int updateCount, long insertOID) {
    this.rs = null;
    this.updateCount = updateCount;
    this.insertOID = insertOID;
  }

  public VxResultSet getResultSet() {
    return rs;
  }

  public int getUpdateCount() {
    return updateCount;
  }

  public long getInsertOID() {
    return insertOID;
  }

  public VxResultWrapper getNext() {
    return next;
  }

  public void append(VxResultWrapper newResult) {
    VxResultWrapper tail = this;
    while (tail.next != null) {
      tail = tail.next;
    }

    tail.next = newResult;
  }

  private final VxResultSet rs;
  private final int updateCount;
  private final long insertOID;
  private VxResultWrapper next;
}
