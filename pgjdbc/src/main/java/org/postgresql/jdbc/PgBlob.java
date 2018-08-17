/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.jdbc;

import org.postgresql.largeobject.LargeObject;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class PgBlob extends AbstractBlobClob implements java.sql.Blob {

  public PgBlob(org.postgresql.core.BaseConnection conn, long oid) throws SQLException {
    super(conn, oid);
  }

  public synchronized java.io.InputStream getBinaryStream(long pos, long length)
      throws SQLException {
    checkFreed();
	try {
		LargeObject subLO = getLo(false).get().copy();
		addSubLO(subLO);
	    if (pos > Integer.MAX_VALUE) {
	      subLO.seek64(pos - 1, LargeObject.SEEK_SET).get();
	    } else {
	      subLO.seek((int) pos - 1, LargeObject.SEEK_SET).get();
	    }
	    return subLO.getInputStream(length);
	} catch (InterruptedException e) {
		throw new PSQLException(GT.tr("Unexpected error get pg blog to database."),
				PSQLState.UNEXPECTED_ERROR, e);
	} catch (ExecutionException e) {
		throw new PSQLException(GT.tr("Unexpected error get pg blog to database."),
				PSQLState.UNEXPECTED_ERROR, e);
	}
  }

  public synchronized int setBytes(long pos, byte[] bytes) throws SQLException {
    return setBytes(pos, bytes, 0, bytes.length);
  }

  public synchronized int setBytes(long pos, byte[] bytes, int offset, int len)
      throws SQLException {
    assertPosition(pos);
    try {
		getLo(true).get().seek((int) (pos - 1)).get();
	} catch (InterruptedException | ExecutionException e) {
		throw new SQLException(e);
	}
    try {
		getLo(true).get().write(bytes, offset, len);
	} catch (InterruptedException | ExecutionException e) {
		throw new SQLException(e);
	}
    return len;
  }
}
