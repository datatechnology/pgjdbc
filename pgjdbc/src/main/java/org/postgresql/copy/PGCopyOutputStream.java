/*
 * Copyright (c) 2009, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.copy;

import org.postgresql.PGConnection;
import org.postgresql.util.GT;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import static com.ea.async.Async.await;

/**
 * OutputStream for buffered input into a PostgreSQL COPY FROM STDIN operation
 */
public class PGCopyOutputStream extends OutputStream implements CopyIn {
  private CopyIn op;
  private final byte[] copyBuffer;
  private final byte[] singleByteBuffer = new byte[1];
  private int at = 0;

  /**
   * Uses given connection for specified COPY FROM STDIN operation
   *
   * @param connection database connection to use for copying (protocol version 3 required)
   * @param sql        COPY FROM STDIN statement
   * @throws SQLException if initializing the operation fails
 * @throws ExecutionException 
 * @throws InterruptedException 
   */
  public PGCopyOutputStream(PGConnection connection, String sql) throws SQLException, InterruptedException, ExecutionException {
    this(connection, sql, CopyManager.DEFAULT_BUFFER_SIZE);
  }

  /**
   * Uses given connection for specified COPY FROM STDIN operation
   *
   * @param connection database connection to use for copying (protocol version 3 required)
   * @param sql        COPY FROM STDIN statement
   * @param bufferSize try to send this many bytes at a time
   * @throws SQLException if initializing the operation fails
 * @throws ExecutionException 
 * @throws InterruptedException 
   */
  public PGCopyOutputStream(PGConnection connection, String sql, int bufferSize)
      throws SQLException, InterruptedException, ExecutionException {
    this(connection.getCopyAPI().copyIn(sql).get(), bufferSize);
  }

  /**
   * Use given CopyIn operation for writing
   *
   * @param op COPY FROM STDIN operation
   */
  public PGCopyOutputStream(CopyIn op) {
    this(op, CopyManager.DEFAULT_BUFFER_SIZE);
  }

  /**
   * Use given CopyIn operation for writing
   *
   * @param op         COPY FROM STDIN operation
   * @param bufferSize try to send this many bytes at a time
   */
  public PGCopyOutputStream(CopyIn op, int bufferSize) {
    this.op = op;
    copyBuffer = new byte[bufferSize];
  }

  public void write(int b) throws IOException {
    checkClosed();
    if (b < 0 || b > 255) {
      throw new IOException(GT.tr("Cannot write to copy a byte of value {0}", b));
    }
    singleByteBuffer[0] = (byte) b;
    write(singleByteBuffer, 0, 1);
  }

  public void write(byte[] buf) throws IOException {
    write(buf, 0, buf.length);
  }

  public void write(byte[] buf, int off, int siz) throws IOException {
    checkClosed();
    try {
      try {
		writeToCopy(buf, off, siz).get();
	} catch (InterruptedException | ExecutionException e) {
		IOException ioe = new IOException("Vertx Write to copy failed.");
	      ioe.initCause(e);
	      throw ioe;
	}
    } catch (SQLException se) {
      IOException ioe = new IOException("Write to copy failed.");
      ioe.initCause(se);
      throw ioe;
    }
  }

  private void checkClosed() throws IOException {
    if (op == null) {
      throw new IOException(GT.tr("This copy stream is closed."));
    }
  }

  public void close() throws IOException {
    // Don't complain about a double close.
    if (op == null) {
      return;
    }

    try {
      try {
		endCopy().get();
	} catch (InterruptedException | ExecutionException e) {
		throw new SQLException(e);
	}
    } catch (SQLException se) {
      IOException ioe = new IOException("Ending write to copy failed.");
      ioe.initCause(se);
      throw ioe;
    }
    op = null;
  }

  public void flush() throws IOException {
    try {
      op.writeToCopy(copyBuffer, 0, at);
      at = 0;
      try {
		op.flushCopy().get();
	} catch (InterruptedException | ExecutionException e) {
		throw new IOException(e);
	}
    } catch (SQLException e) {
      IOException ioe = new IOException("Unable to flush stream");
      ioe.initCause(e);
      throw ioe;
    }
  }

  public CompletableFuture<Void> writeToCopy(byte[] buf, int off, int siz) throws SQLException {
    if (at > 0
        && siz > copyBuffer.length - at) { // would not fit into rest of our buf, so flush buf
      await(op.writeToCopy(copyBuffer, 0, at));
      at = 0;
    }
    if (siz > copyBuffer.length) { // would still not fit into buf, so just pass it through
      await(op.writeToCopy(buf, off, siz));
    } else { // fits into our buf, so save it there
      System.arraycopy(buf, off, copyBuffer, at, siz);
      at += siz;
    }
	return CompletableFuture.completedFuture(null);
  }

  public int getFormat() {
    return op.getFormat();
  }

  public int getFieldFormat(int field) {
    return op.getFieldFormat(field);
  }

  public CompletableFuture<Void> cancelCopy() throws SQLException {
    return op.cancelCopy();
  }

  public int getFieldCount() {
    return op.getFieldCount();
  }

  public boolean isActive() {
    return op.isActive();
  }

  public CompletableFuture<Void> flushCopy() throws SQLException {
    return op.flushCopy();
  }

  public CompletableFuture<Long> endCopy() throws SQLException {
    if (at > 0) {
      await(op.writeToCopy(copyBuffer, 0, at));
    }
    await(op.endCopy());
    return CompletableFuture.completedFuture(getHandledRowCount());
  }

  public long getHandledRowCount() {
    return op.getHandledRowCount();
  }

}
