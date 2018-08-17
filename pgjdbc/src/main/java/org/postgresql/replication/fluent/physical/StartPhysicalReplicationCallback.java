/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.replication.fluent.physical;

import org.postgresql.replication.PGReplicationStream;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

public interface StartPhysicalReplicationCallback {
  CompletableFuture<PGReplicationStream> start(PhysicalReplicationOptions options) throws SQLException;
}
