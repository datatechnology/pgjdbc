/*
 * Copyright (c) 2009, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.copy;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

/**
 * Copy bulk data from client into a PostgreSQL table very fast.
 */
public interface CopyIn extends CopyOperation {

  /**
   * Writes specified part of given byte array to an open and writable copy operation.
   *
   * @param buf array of bytes to write
   * @param off offset of first byte to write (normally zero)
   * @param siz number of bytes to write (normally buf.length)
 * @return 
   * @throws SQLException if the operation fails
   */
  CompletableFuture<Void> writeToCopy(byte[] buf, int off, int siz) throws SQLException;

  /**
   * Force any buffered output to be sent over the network to the backend. In general this is a
   * useless operation as it will get pushed over in due time or when endCopy is called. Some
   * specific modified server versions (Truviso) want this data sooner. If you are unsure if you
   * need to use this method, don't.
 * @return 
   *
   * @throws SQLException if the operation fails.
   */
  CompletableFuture<Void> flushCopy() throws SQLException;

  /**
   * Finishes copy operation succesfully.
   *
   * @return number of updated rows for server 8.2 or newer (see getHandledRowCount())
   * @throws SQLException if the operation fails.
   */
  CompletableFuture<Long> endCopy() throws SQLException;
}
