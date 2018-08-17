/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core.v3;

import org.postgresql.copy.CopyDual;
import org.postgresql.util.PSQLException;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import static com.ea.async.Async.await;

public class CopyDualImpl extends CopyOperationImpl implements CopyDual {
  private Queue<byte[]> received = new LinkedList<byte[]>();

  public CompletableFuture<Void> writeToCopy(byte[] data, int off, int siz) throws SQLException {
    return queryExecutor.writeToCopy(this, data, off, siz);
  }

  public CompletableFuture<Void> flushCopy() throws SQLException {
	  return queryExecutor.flushCopy(this);
  }

  public CompletableFuture<Long> endCopy() throws SQLException {
    return queryExecutor.endCopy(this);
  }

  public CompletableFuture<byte[]> readFromCopy() throws SQLException {
    if (received.isEmpty()) {
      await(queryExecutor.readFromCopy(this, true));
    }

    return CompletableFuture.completedFuture(received.poll());
  }

  @Override
  public CompletableFuture<byte[]> readFromCopy(boolean block) throws SQLException {
    if (received.isEmpty()) {
      await(queryExecutor.readFromCopy(this, block));
    }

    return CompletableFuture.completedFuture(received.poll());
  }

  @Override
  public void handleCommandStatus(String status) throws PSQLException {
  }

  protected void handleCopydata(byte[] data) {
    received.add(data);
  }
}
