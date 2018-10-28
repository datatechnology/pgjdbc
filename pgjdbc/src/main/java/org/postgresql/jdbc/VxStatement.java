/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.jdbc;

import org.postgresql.Driver;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.BaseStatement;
import org.postgresql.core.CachedQuery;
import org.postgresql.core.Field;
import org.postgresql.core.ParameterList;
import org.postgresql.core.Query;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.ResultCursor;
import org.postgresql.core.ResultHandlerBase;
import org.postgresql.core.SqlCommand;
import org.postgresql.core.VxBaseStatement;
import org.postgresql.core.v3.QueryExecutorImpl;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

//import java.sql.Connection;
//import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
//import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.ea.async.Async.await;

public class VxStatement implements VxBaseStatement {
  private static final Logger LOGGER = Logger.getLogger(VxStatement.class.getName());
	private static final String[] NO_RETURNING_COLUMNS = new String[0];

	/**
	 * Default state for use or not binary transfers. Can use only for testing
	 * purposes
	 */
	private static final boolean DEFAULT_FORCE_BINARY_TRANSFERS = Boolean.getBoolean("org.postgresql.forceBinary");
	// only for testing purposes. even single shot statements will use binary
	// transfers
	private boolean forceBinaryTransfers = DEFAULT_FORCE_BINARY_TRANSFERS;

	protected ArrayList<Query> batchStatements = null;
	protected ArrayList<ParameterList> batchParameters = null;
	protected final int resultsettype; // the resultset type to return (ResultSet.TYPE_xxx)
	protected final int concurrency; // is it updateable or not? (ResultSet.CONCUR_xxx)
	private final int rsHoldability;
	private boolean poolable;
	private boolean closeOnCompletion = false;
	protected int fetchdirection = java.sql.ResultSet.FETCH_FORWARD;
	// fetch direction hint (currently ignored)

	/**
	 * Protects current statement from cancelTask starting, waiting for a bit, and
	 * waking up exactly on subsequent query execution. The idea is to atomically
	 * compare and swap the reference to the task, so the task can detect that
	 * statement executes different query than the one the cancelTask was created.
	 * Note: the field must be set/get/compareAndSet via
	 * {@link #CANCEL_TIMER_UPDATER} as per {@link AtomicReferenceFieldUpdater}
	 * javadoc.
	 */
	private volatile TimerTask cancelTimerTask = null;
	private static final AtomicReferenceFieldUpdater<VxStatement, TimerTask> CANCEL_TIMER_UPDATER = AtomicReferenceFieldUpdater
			.newUpdater(VxStatement.class, TimerTask.class, "cancelTimerTask");

	/**
	 * Protects statement from out-of-order cancels. It protects from both
	 * {@link #setQueryTimeout(int)} and {@link #cancel()} induced ones.
	 *
	 * {@link #execute(String)} and friends change the field to
	 * {@link StatementCancelState#IN_QUERY} during execute. {@link #cancel()}
	 * ignores cancel request if state is {@link StatementCancelState#IDLE}. In case
	 * {@link #execute(String)} observes non-{@link StatementCancelState#IDLE} state
	 * as it completes the query, it waits till
	 * {@link StatementCancelState#CANCELLED}. Note: the field must be
	 * set/get/compareAndSet via {@link #STATE_UPDATER} as per
	 * {@link AtomicIntegerFieldUpdater} javadoc.
	 */
	private volatile StatementCancelState statementState = StatementCancelState.IDLE;

	private static final AtomicReferenceFieldUpdater<VxStatement, StatementCancelState> STATE_UPDATER = AtomicReferenceFieldUpdater
			.newUpdater(VxStatement.class, StatementCancelState.class, "statementState");

	/**
	 * Does the caller of execute/executeUpdate want generated keys for this
	 * execution? This is set by Statement methods that have generated keys
	 * arguments and cleared after execution is complete.
	 */
	protected boolean wantsGeneratedKeysOnce = false;

	/**
	 * Was this PreparedStatement created to return generated keys for every
	 * execution? This is set at creation time and never cleared by execution.
	 */
	public boolean wantsGeneratedKeysAlways = false;

	// The connection who created us
	protected final VxConnection connection;

	/**
	 * The warnings chain.
	 */
	protected volatile PSQLWarningWrapper warnings = null;

	/**
	 * Maximum number of rows to return, 0 = unlimited
	 */
	protected int maxrows = 0;

	/**
	 * Number of rows to get in a batch.
	 */
	protected int fetchSize = 0;

	/**
	 * Timeout (in milliseconds) for a query
	 */
	protected long timeout = 0;

	protected boolean replaceProcessingEnabled = true;

	/**
	 * The current results.
	 */
	protected VxResultWrapper result = null;

	/**
	 * The first unclosed result.
	 */
	protected VxResultWrapper firstUnclosedResult = null;

	/**
	 * Results returned by a statement that wants generated keys.
	 */
	protected VxResultWrapper generatedKeys = null;

	protected int m_prepareThreshold; // Reuse threshold to enable use of PREPARE

	protected int maxfieldSize = 0;

	VxStatement(VxConnection connection2, int rsType, int rsConcurrency, int rsHoldability) throws SQLException {
		this.connection = connection2;
		forceBinaryTransfers |= connection2.getForceBinary();
		resultsettype = rsType;
		concurrency = rsConcurrency;
		setFetchSize(connection2.getDefaultFetchSize());
		setPrepareThreshold(connection2.getPrepareThreshold());
		this.rsHoldability = rsHoldability;
	}

	public VxResultSet createResultSet(Query originalQuery, Field[] fields, List<byte[][]> tuples, ResultCursor cursor)
			throws SQLException {
		VxResultSet newResult = new VxResultSet(originalQuery, this, fields, tuples, cursor, getMaxRows(),
				getMaxFieldSize(), getResultSetType(), getResultSetConcurrency(), getResultSetHoldability());
		newResult.setFetchSize(getFetchSize());
		newResult.setFetchDirection(getFetchDirection());
		return newResult;
	}

	public VxConnection getPGConnection() {
		return connection;
	}

	public String getFetchingCursorName() {
		return null;
	}

	public int getFetchSize() {
		return fetchSize;
	}

	protected boolean wantsScrollableResultSet() {
		return resultsettype != java.sql.ResultSet.TYPE_FORWARD_ONLY;
	}

	protected boolean wantsHoldableResultSet() {
		// FIXME: false if not supported
		return rsHoldability == java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	/**
	 * ResultHandler implementations for updates, queries, and either-or.
	 */
	public class VxStatementResultHandler extends ResultHandlerBase {
		private VxResultWrapper results;
		private VxResultWrapper lastResult;

		VxResultWrapper getResults() {
			return results;
		}

		private void append(VxResultWrapper newResult) {
			if (results == null) {
				lastResult = results = newResult;
			} else {
				lastResult.append(newResult);
			}
		}

		@Override
		public void handleResultRows(Query fromQuery, Field[] fields, List<byte[][]> tuples, ResultCursor cursor) {
			try {
				VxResultSet rs = VxStatement.this.createResultSet(fromQuery, fields, tuples, cursor);
				append(new VxResultWrapper(rs));
			} catch (SQLException e) {
				handleError(e);
			}
		}

		@Override
		public void handleCommandStatus(String status, int updateCount, long insertOID) {
			append(new VxResultWrapper(updateCount, insertOID));
		}

		@Override
		public void handleWarning(SQLWarning warning) {
			VxStatement.this.addWarning(warning);
		}

	}

	public CompletableFuture<VxResultSet> executeQuery(String p_sql) throws SQLException {
		if (!await(executeWithFlags(p_sql, 0))) {
			throw new PSQLException(GT.tr("No results were returned by the query."), PSQLState.NO_DATA);
		}
		return CompletableFuture.completedFuture(getSingleResultSet());
	}

	protected VxResultSet getSingleResultSet() throws SQLException {
		synchronized (this) {
			checkClosed();
			if (result.getNext() != null) {
				throw new PSQLException(GT.tr("Multiple ResultSets were returned by the query."),
						PSQLState.TOO_MANY_RESULTS);
			}

			return result.getResultSet();
		}
	}

	public CompletableFuture<Integer> executeUpdate(String p_sql) throws SQLException {
	  await(executeWithFlags(p_sql, QueryExecutor.QUERY_NO_RESULTS));
		return CompletableFuture.completedFuture(getNoResultUpdateCount());
	}

	protected int getNoResultUpdateCount() throws SQLException {
		synchronized (this) {
			checkClosed();
			VxResultWrapper iter = result;
			while (iter != null) {
				if (iter.getResultSet() != null) {
					throw new PSQLException(GT.tr("A result was returned when none was expected."),
							PSQLState.TOO_MANY_RESULTS);

				}
				iter = iter.getNext();
			}

			return getUpdateCount();
		}
	}

	public CompletableFuture<Boolean> execute(String p_sql) throws SQLException {
		return executeWithFlags(p_sql, 0);
	}

	public CompletableFuture<Boolean> executeWithFlags(String sql, int flags) throws SQLException {
		return executeCachedSql(sql, flags, NO_RETURNING_COLUMNS);
	}

	private CompletableFuture<Boolean> executeCachedSql(String sql, int flags, String[] columnNames) throws SQLException {
		PreferQueryMode preferQueryMode = connection.getPreferQueryMode();
		// Simple statements should not replace ?, ? with $1, $2
		boolean shouldUseParameterized = false;
		QueryExecutor queryExecutor = connection.getQueryExecutor();
		Object key = queryExecutor.createQueryKey(sql, replaceProcessingEnabled, shouldUseParameterized, columnNames);
		CachedQuery cachedQuery;
		boolean shouldCache = preferQueryMode == PreferQueryMode.EXTENDED_CACHE_EVERYTHING;
		if (shouldCache) {
			cachedQuery = queryExecutor.borrowQueryByKey(key);
		} else {
			cachedQuery = queryExecutor.createQueryByKey(key);
		}
		if (wantsGeneratedKeysOnce) {
			SqlCommand sqlCommand = cachedQuery.query.getSqlCommand();
			wantsGeneratedKeysOnce = sqlCommand != null && sqlCommand.isReturningKeywordPresent();
		}
		boolean res;
		try {
			res = await(executeWithFlags(cachedQuery, flags));
		} finally {
			if (shouldCache) {
				queryExecutor.releaseQuery(cachedQuery);
			}
		}
		return CompletableFuture.completedFuture(res);
	}

	public CompletableFuture<Boolean> executeWithFlags(CachedQuery simpleQuery, int flags) throws SQLException {
		checkClosed();
		if (connection.getPreferQueryMode().compareTo(PreferQueryMode.EXTENDED) < 0) {
			flags |= QueryExecutor.QUERY_EXECUTE_AS_SIMPLE;
		}
		await(execute(simpleQuery, null, flags));
		synchronized (this) {
			checkClosed();
			return CompletableFuture.completedFuture((result != null && result.getResultSet() != null));
		}
	}

	public CompletableFuture<Boolean> executeWithFlags(int flags) throws SQLException {
		checkClosed();
		throw new PSQLException(GT.tr("Can''t use executeWithFlags(int) on a Statement."), PSQLState.WRONG_OBJECT_TYPE);
	}

	protected void closeForNextExecution() throws SQLException {
		// Every statement execution clears any previous warnings.
		clearWarnings();

		// Close any existing resultsets associated with this statement.
		synchronized (this) {
			while (firstUnclosedResult != null) {
				VxResultSet rs = firstUnclosedResult.getResultSet();
				if (rs != null) {
					rs.close();
				}
				firstUnclosedResult = firstUnclosedResult.getNext();
			}
			result = null;

			if (generatedKeys != null) {
				if (generatedKeys.getResultSet() != null) {
					generatedKeys.getResultSet().close();
				}
				generatedKeys = null;
			}
		}
	}

	/**
	 * Returns true if query is unlikely to be reused
	 *
	 * @param cachedQuery
	 *            to check (null if current query)
	 * @return true if query is unlikely to be reused
	 */
	protected boolean isOneShotQuery(CachedQuery cachedQuery) {
		if (cachedQuery == null) {
			return true;
		}
		cachedQuery.increaseExecuteCount();
		if ((m_prepareThreshold == 0 || cachedQuery.getExecuteCount() < m_prepareThreshold)
				&& !getForceBinaryTransfer()) {
			return true;
		}
		return false;
	}

	protected final CompletableFuture<Void> execute(CachedQuery cachedQuery, ParameterList queryParameters, int flags)
			throws SQLException {
		try {
			await(executeInternal(cachedQuery, queryParameters, flags));
		} catch (SQLException e) {
			// Don't retry composite queries as it might get partially executed
		  LOGGER.log(Level.FINEST, cachedQuery.query.getSubqueries() == null ? "DEBUG---NULL" : "DEBUG---NOT NULL");
		  LOGGER.log(Level.FINEST, connection.getQueryExecutor().willHealOnRetry(e) ? "Retry" : "NORetry");
			if (cachedQuery.query.getSubqueries() != null || !connection.getQueryExecutor().willHealOnRetry(e)) {
				throw e;
				// result.completeExceptionally(e);
			}
			cachedQuery.query.close();
			// Execute the query one more time
			await(executeInternal(cachedQuery, queryParameters, flags));
		}
		
		return CompletableFuture.completedFuture(null);
	}

	private CompletableFuture<Void> executeInternal(CachedQuery cachedQuery, ParameterList queryParameters, int flags)
			throws SQLException {
		closeForNextExecution();

		// Enable cursor-based resultset if possible.
		if (fetchSize > 0 && !wantsScrollableResultSet() && !connection.getAutoCommit() && !wantsHoldableResultSet()) {
			flags |= QueryExecutor.QUERY_FORWARD_CURSOR;
		}

		if (wantsGeneratedKeysOnce || wantsGeneratedKeysAlways) {
			flags |= QueryExecutor.QUERY_BOTH_ROWS_AND_STATUS;

			// If the no results flag is set (from executeUpdate)
			// clear it so we get the generated keys results.
			//
			if ((flags & QueryExecutor.QUERY_NO_RESULTS) != 0) {
				flags &= ~(QueryExecutor.QUERY_NO_RESULTS);
			}
		}

		if (isOneShotQuery(cachedQuery)) {
			flags |= QueryExecutor.QUERY_ONESHOT;
		}
		// Only use named statements after we hit the threshold. Note that only
		// named statements can be transferred in binary format.

		if (connection.getAutoCommit()) {
			flags |= QueryExecutor.QUERY_SUPPRESS_BEGIN;
		}

		// updateable result sets do not yet support binary updates
		if (concurrency != java.sql.ResultSet.CONCUR_READ_ONLY) {
			flags |= QueryExecutor.QUERY_NO_BINARY_TRANSFER;
		}

		Query queryToExecute = cachedQuery.query;

		if (queryToExecute.isEmpty()) {
			flags |= QueryExecutor.QUERY_SUPPRESS_BEGIN;
		}
		
		if (!queryToExecute.isStatementDescribed() && forceBinaryTransfers
				&& (flags & QueryExecutor.QUERY_EXECUTE_AS_SIMPLE) == 0) {
			// Simple 'Q' execution does not need to know parameter types
			// When binaryTransfer is forced, then we need to know resulting parameter and
			// column types,
			// thus sending a describe request.
			int flags2 = flags | QueryExecutor.QUERY_DESCRIBE_ONLY;
			VxStatementResultHandler handler2 = new VxStatementResultHandler();
			await(connection.getQueryExecutor().execute(queryToExecute, queryParameters, handler2, 0, 0, flags2));

			VxResultWrapper result2 = handler2.getResults();
			if (result2 != null) {
				result2.getResultSet().close();
			}
		}

		VxStatementResultHandler handler = new VxStatementResultHandler();
		synchronized (this) {
			result = null;
		}
		try {
			startTimer();
			await(connection.getQueryExecutor().execute(queryToExecute, queryParameters, handler, maxrows, fetchSize,
					flags));

		} finally {
			killTimerTask();
		}
		synchronized (this) {
			checkClosed();
			result = firstUnclosedResult = handler.getResults();

			if (wantsGeneratedKeysOnce || wantsGeneratedKeysAlways) {
				generatedKeys = result;
				result = result.getNext();

				if (wantsGeneratedKeysOnce) {
					wantsGeneratedKeysOnce = false;
				}
			}
		}
		return CompletableFuture.completedFuture(null);
	}

	public void setCursorName(String name) throws SQLException {
		checkClosed();
		// No-op.
	}

	private volatile boolean isClosed = false;

	public int getUpdateCount() throws SQLException {
		synchronized (this) {
			checkClosed();
			if (result == null || result.getResultSet() != null) {
				return -1;
			}

			return result.getUpdateCount();
		}
	}

	public boolean getMoreResults() throws SQLException {
		synchronized (this) {
			checkClosed();
			if (result == null) {
				return false;
			}

			result = result.getNext();

			// Close preceding resultsets.
			while (firstUnclosedResult != result) {
				if (firstUnclosedResult.getResultSet() != null) {
					firstUnclosedResult.getResultSet().close();
				}
				firstUnclosedResult = firstUnclosedResult.getNext();
			}

			return (result != null && result.getResultSet() != null);
		}
	}

	public int getMaxRows() throws SQLException {
		checkClosed();
		return maxrows;
	}

	public void setMaxRows(int max) throws SQLException {
		checkClosed();
		if (max < 0) {
			throw new PSQLException(GT.tr("Maximum number of rows must be a value grater than or equal to 0."),
					PSQLState.INVALID_PARAMETER_VALUE);
		}
		maxrows = max;
	}

	public void setEscapeProcessing(boolean enable) throws SQLException {
		checkClosed();
		replaceProcessingEnabled = enable;
	}

	public int getQueryTimeout() throws SQLException {
		checkClosed();
		long seconds = timeout / 1000;
		if (seconds >= Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		}
		return (int) seconds;
	}

	public void setQueryTimeout(int seconds) throws SQLException {
		setQueryTimeoutMs(seconds * 1000L);
	}

	/**
	 * The queryTimeout limit is the number of milliseconds the driver will wait for
	 * a Statement to execute. If the limit is exceeded, a SQLException is thrown.
	 *
	 * @return the current query timeout limit in milliseconds; 0 = unlimited
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public long getQueryTimeoutMs() throws SQLException {
		checkClosed();
		return timeout;
	}

	/**
	 * Sets the queryTimeout limit
	 *
	 * @param millis
	 *            - the new query timeout limit in milliseconds
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void setQueryTimeoutMs(long millis) throws SQLException {
		checkClosed();

		if (millis < 0) {
			throw new PSQLException(GT.tr("Query timeout must be a value greater than or equals to 0."),
					PSQLState.INVALID_PARAMETER_VALUE);
		}
		timeout = millis;
	}

	/**
	 * Either initializes new warning wrapper, or adds warning onto the chain.
	 *
	 * Although warnings are expected to be added sequentially, the warnings chain
	 * may be cleared concurrently at any time via {@link #clearWarnings()},
	 * therefore it is possible that a warning added via this method is placed onto
	 * the end of the previous warning chain
	 *
	 * @param warn
	 *            warning to add
	 */
	public void addWarning(SQLWarning warn) {
		// copy reference to avoid NPE from concurrent modification of this.warnings
		final PSQLWarningWrapper warnWrap = this.warnings;
		if (warnWrap == null) {
			this.warnings = new PSQLWarningWrapper(warn);
		} else {
			warnWrap.addWarning(warn);
		}
	}

	public SQLWarning getWarnings() throws SQLException {
		checkClosed();
		// copy reference to avoid NPE from concurrent modification of this.warnings
		final PSQLWarningWrapper warnWrap = this.warnings;
		return warnWrap != null ? warnWrap.getFirstWarning() : null;
	}

	public int getMaxFieldSize() throws SQLException {
		return maxfieldSize;
	}

	public void setMaxFieldSize(int max) throws SQLException {
		checkClosed();
		if (max < 0) {
			throw new PSQLException(GT.tr("The maximum field size must be a value greater than or equal to 0."),
					PSQLState.INVALID_PARAMETER_VALUE);
		}
		maxfieldSize = max;
	}

	/**
	 * Clears the warning chain.
	 * <p>
	 * Note that while it is safe to clear warnings while the query is executing,
	 * warnings that are added between calls to {@link #getWarnings()} and
	 * #clearWarnings() may be missed. Therefore you should hold a reference to the
	 * tail of the previous warning chain and verify if its
	 * {@link SQLWarning#getNextWarning()} value is holds any new value.
	 */
	public void clearWarnings() throws SQLException {
		warnings = null;
	}

	public VxResultSet getResultSet() throws SQLException {
		synchronized (this) {
			checkClosed();

			if (result == null) {
				return null;
			}

			return result.getResultSet();
		}
	}

	/**
	 * <B>Note:</B> even though {@code Statement} is automatically closed when it is
	 * garbage collected, it is better to close it explicitly to lower resource
	 * consumption.
	 *
	 * {@inheritDoc}
	 */
	public final void close() throws SQLException {
		// closing an already closed Statement is a no-op.
		synchronized (this) {
			if (isClosed) {
				return;
			}
			isClosed = true;
		}

		cancel();

		closeForNextExecution();

		closeImpl();
	}

	/**
	 * This is guaranteed to be called exactly once even in case of concurrent
	 * {@link #close()} calls.
	 * 
	 * @throws SQLException
	 *             in case of error
	 */
	protected void closeImpl() throws SQLException {
	}

	/*
	 *
	 * The following methods are postgres extensions and are defined in the
	 * interface BaseStatement
	 *
	 */

	public long getLastOID() throws SQLException {
		synchronized (this) {
			checkClosed();
			if (result == null) {
				return 0;
			}
			return result.getInsertOID();
		}
	}

	public void setPrepareThreshold(int newThreshold) throws SQLException {
		checkClosed();

		if (newThreshold < 0) {
			forceBinaryTransfers = true;
			newThreshold = 1;
		}

		this.m_prepareThreshold = newThreshold;
	}

	public int getPrepareThreshold() {
		return m_prepareThreshold;
	}

	public void setUseServerPrepare(boolean flag) throws SQLException {
		setPrepareThreshold(flag ? 1 : 0);
	}

	public boolean isUseServerPrepare() {
		return false;
	}

	protected void checkClosed() throws SQLException {
		if (isClosed()) {
			throw new PSQLException(GT.tr("This statement has been closed."), PSQLState.OBJECT_NOT_IN_STATE);
		}
	}

	// ** JDBC 2 Extensions **

	public void addBatch(String p_sql) throws SQLException {
		checkClosed();

		if (batchStatements == null) {
			batchStatements = new ArrayList<Query>();
			batchParameters = new ArrayList<ParameterList>();
		}

		// Simple statements should not replace ?, ? with $1, $2
		boolean shouldUseParameterized = false;
		CachedQuery cachedQuery = connection.createQuery(p_sql, replaceProcessingEnabled, shouldUseParameterized);
		batchStatements.add(cachedQuery.query);
		batchParameters.add(null);
	}

	public void clearBatch() throws SQLException {
		if (batchStatements != null) {
			batchStatements.clear();
			batchParameters.clear();
		}
	}

	protected VxBatchResultHandler createBatchHandler(Query[] queries, ParameterList[] parameterLists) {
		return new VxBatchResultHandler(this, queries, parameterLists, wantsGeneratedKeysAlways);
	}

	public CompletableFuture<int[]> executeBatch() throws SQLException {
		checkClosed();

		closeForNextExecution();

		if (batchStatements == null || batchStatements.isEmpty()) {
			return CompletableFuture.completedFuture(new int[0]);
		}

		// Construct query/parameter arrays.
		transformQueriesAndParameters();
		// Empty arrays should be passed to toArray
		// see http://shipilev.net/blog/2016/arrays-wisdom-ancients/
		Query[] queries = batchStatements.toArray(new Query[0]);
		ParameterList[] parameterLists = batchParameters.toArray(new ParameterList[0]);
		batchStatements.clear();
		batchParameters.clear();

		int flags = 0;

		// Force a Describe before any execution? We need to do this if we're going
		// to send anything dependent on the Describe results, e.g. binary parameters.
		boolean preDescribe = false;

		if (wantsGeneratedKeysAlways) {
			/*
			 * This batch will return generated keys, tell the executor to expect result
			 * rows. We also force a Describe later so we know the size of the results to
			 * expect.
			 *
			 * If the parameter type(s) change between batch entries and the default
			 * binary-mode changes we might get mixed binary and text in a single result set
			 * column, which we cannot handle. To prevent this, disable binary transfer mode
			 * in batches that return generated keys. See GitHub issue #267
			 */
			flags = QueryExecutor.QUERY_BOTH_ROWS_AND_STATUS | QueryExecutor.QUERY_NO_BINARY_TRANSFER;
		} else {
			// If a batch hasn't specified that it wants generated keys, using the
			// appropriate
			// Connection.createStatement(...) interfaces, disallow any result set.
			flags = QueryExecutor.QUERY_NO_RESULTS;
		}

		PreferQueryMode preferQueryMode = connection.getPreferQueryMode();
		if (preferQueryMode == PreferQueryMode.SIMPLE
				|| (preferQueryMode == PreferQueryMode.EXTENDED_FOR_PREPARED && parameterLists[0] == null)) {
			flags |= QueryExecutor.QUERY_EXECUTE_AS_SIMPLE;
		}

		boolean sameQueryAhead = queries.length > 1 && queries[0] == queries[1];

		if (!sameQueryAhead
				// If executing the same query twice in a batch, make sure the statement
				// is server-prepared. In other words, "oneshot" only if the query is one in the
				// batch
				// or the queries are different
				|| isOneShotQuery(null)) {
			flags |= QueryExecutor.QUERY_ONESHOT;
		} else {
			// If a batch requests generated keys and isn't already described,
			// force a Describe of the query before proceeding. That way we can
			// determine the appropriate size of each batch by estimating the
			// maximum data returned. Without that, we don't know how many queries
			// we'll be able to queue up before we risk a deadlock.
			// (see v3.QueryExecutorImpl's MAX_BUFFERED_RECV_BYTES)

			// SameQueryAhead is just a quick way to issue pre-describe for batch execution
			// TODO: It should be reworked into "pre-describe if query has unknown parameter
			// types and same query is ahead".
			preDescribe = (wantsGeneratedKeysAlways || sameQueryAhead) && !queries[0].isStatementDescribed();
			/*
			 * It's also necessary to force a Describe on the first execution of the new
			 * statement, even though we already described it, to work around bug #267.
			 */
			flags |= QueryExecutor.QUERY_FORCE_DESCRIBE_PORTAL;
		}

		if (connection.getAutoCommit()) {
			flags |= QueryExecutor.QUERY_SUPPRESS_BEGIN;
		}

		VxBatchResultHandler handler;
		handler = createBatchHandler(queries, parameterLists);

		if ((preDescribe || forceBinaryTransfers) && (flags & QueryExecutor.QUERY_EXECUTE_AS_SIMPLE) == 0) {
			// Do a client-server round trip, parsing and describing the query so we
			// can determine its result types for use in binary parameters, batch sizing,
			// etc.
			int flags2 = flags | QueryExecutor.QUERY_DESCRIBE_ONLY;
			VxStatementResultHandler handler2 = new VxStatementResultHandler();
			try {
				await(connection.getQueryExecutor().execute(queries[0], parameterLists[0], handler2, 0, 0, flags2));
			} catch (SQLException e) {
				// Unable to parse the first statement -> throw BatchUpdateException
				handler.handleError(e);
				handler.handleCompletion();
				// Will not reach here (see above)
			}
			VxResultWrapper result2 = handler2.getResults();
			if (result2 != null) {
				result2.getResultSet().close();
			}
		}

		synchronized (this) {
			result = null;
		}

		try {
			startTimer();
			await(connection.getQueryExecutor().executeAsync(queries, parameterLists, handler, maxrows, fetchSize, flags));
		} finally {
			killTimerTask();
			// There might be some rows generated even in case of failures
			synchronized (this) {
				checkClosed();
				if (wantsGeneratedKeysAlways) {
					generatedKeys = new VxResultWrapper(handler.getGeneratedKeys());
				}
			}
		}

		return CompletableFuture.completedFuture(handler.getUpdateCount());
	}

	public void cancel() throws SQLException {
		if (statementState == StatementCancelState.IDLE) {
			return;
		}
		if (!STATE_UPDATER.compareAndSet(this, StatementCancelState.IN_QUERY, StatementCancelState.CANCELING)) {
			// Not in query, there's nothing to cancel
			return;
		}
		// Synchronize on connection to avoid spinning in killTimerTask
		synchronized (connection) {
			try {
				try {
					connection.cancelQuery().get();
				} catch (InterruptedException | ExecutionException e) {
					throw new SQLException(e);
				}
			} finally {
				STATE_UPDATER.set(this, StatementCancelState.CANCELLED);
				connection.notifyAll(); // wake-up killTimerTask
			}
		}
	}

	public VxConnection getConnection() throws SQLException {
		return connection;
	}

	public int getFetchDirection() {
		return fetchdirection;
	}

	public int getResultSetConcurrency() {
		return concurrency;
	}

	public int getResultSetType() {
		return resultsettype;
	}

	public void setFetchDirection(int direction) throws SQLException {
		switch (direction) {
		case java.sql.ResultSet.FETCH_FORWARD:
		case java.sql.ResultSet.FETCH_REVERSE:
		case java.sql.ResultSet.FETCH_UNKNOWN:
			fetchdirection = direction;
			break;
		default:
			throw new PSQLException(GT.tr("Invalid fetch direction constant: {0}.", direction),
					PSQLState.INVALID_PARAMETER_VALUE);
		}
	}

	public void setFetchSize(int rows) throws SQLException {
		checkClosed();
		if (rows < 0) {
			throw new PSQLException(GT.tr("Fetch size must be a value greater to or equal to 0."),
					PSQLState.INVALID_PARAMETER_VALUE);
		}
		fetchSize = rows;
	}

	private void startTimer() {
		/*
		 * there shouldn't be any previous timer active, but better safe than sorry.
		 */
		cleanupTimer();

		STATE_UPDATER.set(this, StatementCancelState.IN_QUERY);

		if (timeout == 0) {
			return;
		}

		TimerTask cancelTask = new TimerTask() {
			public void run() {
				try {
					if (!CANCEL_TIMER_UPDATER.compareAndSet(VxStatement.this, this, null)) {
						// Nothing to do here, statement has already finished and cleared
						// cancelTimerTask reference
						return;
					}
					VxStatement.this.cancel();
				} catch (SQLException e) {
				}
			}
		};

		CANCEL_TIMER_UPDATER.set(this, cancelTask);
		connection.addTimerTask(cancelTask, timeout);
	}

	/**
	 * Clears {@link #cancelTimerTask} if any. Returns true if and only if "cancel"
	 * timer task would never invoke {@link #cancel()}.
	 */
	private boolean cleanupTimer() {
		TimerTask timerTask = CANCEL_TIMER_UPDATER.get(this);
		if (timerTask == null) {
			// If timeout is zero, then timer task did not exist, so we safely report "all
			// clear"
			return timeout == 0;
		}
		if (!CANCEL_TIMER_UPDATER.compareAndSet(this, timerTask, null)) {
			// Failed to update reference -> timer has just fired, so we must wait for the
			// query state to
			// become "cancelling".
			return false;
		}
		timerTask.cancel();
		connection.purgeTimerTasks();
		// All clear
		return true;
	}

	private void killTimerTask() {
		boolean timerTaskIsClear = cleanupTimer();
		// The order is important here: in case we need to wait for the cancel task, the
		// state must be
		// kept StatementCancelState.IN_QUERY, so cancelTask would be able to cancel the
		// query.
		// It is believed that this case is very rare, so "additional cancel and wait
		// below" would not
		// harm it.
		if (timerTaskIsClear
				&& STATE_UPDATER.compareAndSet(this, StatementCancelState.IN_QUERY, StatementCancelState.IDLE)) {
			return;
		}

		// Being here means someone managed to call .cancel() and our connection did not
		// receive
		// "timeout error"
		// We wait till state becomes "cancelled"
		boolean interrupted = false;
		synchronized (connection) {
			// state check is performed under synchronized so it detects "cancelled" state
			// faster
			// In other words, it prevents unnecessary ".wait()" call
			while (!STATE_UPDATER.compareAndSet(this, StatementCancelState.CANCELLED, StatementCancelState.IDLE)) {
				try {
					// Note: wait timeout here is irrelevant since synchronized(connection) would
					// block until
					// .cancel finishes
					connection.wait(10);
				} catch (InterruptedException e) { // NOSONAR
					// Either re-interrupt this method or rethrow the "InterruptedException"
					interrupted = true;
				}
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	protected boolean getForceBinaryTransfer() {
		return forceBinaryTransfers;
	}

	public long getLargeUpdateCount() throws SQLException {
		throw Driver.notImplemented(this.getClass(), "getLargeUpdateCount");
	}

	public void setLargeMaxRows(long max) throws SQLException {
		throw Driver.notImplemented(this.getClass(), "setLargeMaxRows");
	}

	public long getLargeMaxRows() throws SQLException {
		throw Driver.notImplemented(this.getClass(), "getLargeMaxRows");
	}

	public long[] executeLargeBatch() throws SQLException {
		throw Driver.notImplemented(this.getClass(), "executeLargeBatch");
	}

	public long executeLargeUpdate(String sql) throws SQLException {
		throw Driver.notImplemented(this.getClass(), "executeLargeUpdate");
	}

	public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		throw Driver.notImplemented(this.getClass(), "executeLargeUpdate");
	}

	public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
		throw Driver.notImplemented(this.getClass(), "executeLargeUpdate");
	}

	public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
		throw Driver.notImplemented(this.getClass(), "executeLargeUpdate");
	}

	public long executeLargeUpdate() throws SQLException {
		throw Driver.notImplemented(this.getClass(), "executeLargeUpdate");
	}

	public boolean isClosed() throws SQLException {
		return isClosed;
	}

	public void setPoolable(boolean poolable) throws SQLException {
		checkClosed();
		this.poolable = poolable;
	}

	public boolean isPoolable() throws SQLException {
		checkClosed();
		return poolable;
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isAssignableFrom(getClass());
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (iface.isAssignableFrom(getClass())) {
			return iface.cast(this);
		}
		throw new SQLException("Cannot unwrap to " + iface.getName());
	}

	public void closeOnCompletion() throws SQLException {
		closeOnCompletion = true;
	}

	public boolean isCloseOnCompletion() throws SQLException {
		return closeOnCompletion;
	}

	protected void checkCompletion() throws SQLException {
		if (!closeOnCompletion) {
			return;
		}

		synchronized (this) {
			VxResultWrapper result = firstUnclosedResult;
			while (result != null) {
				if (result.getResultSet() != null && !result.getResultSet().isClosed()) {
					return;
				}
				result = result.getNext();
			}
		}

		// prevent all ResultSet.close arising from Statement.close to loop here
		closeOnCompletion = false;
		try {
			close();
		} finally {
			// restore the status if one rely on isCloseOnCompletion
			closeOnCompletion = true;
		}
	}

	public boolean getMoreResults(int current) throws SQLException {
		synchronized (this) {
			checkClosed();
			// CLOSE_CURRENT_RESULT
			if (current == VxBaseStatement.CLOSE_CURRENT_RESULT && result != null && result.getResultSet() != null) {
				result.getResultSet().close();
			}

			// Advance resultset.
			if (result != null) {
				result = result.getNext();
			}

			// CLOSE_ALL_RESULTS
			if (current == VxBaseStatement.CLOSE_ALL_RESULTS) {
				// Close preceding resultsets.
				while (firstUnclosedResult != result) {
					if (firstUnclosedResult.getResultSet() != null) {
						firstUnclosedResult.getResultSet().close();
					}
					firstUnclosedResult = firstUnclosedResult.getNext();
				}
			}

			// Done.
			return (result != null && result.getResultSet() != null);
		}
	}

	public VxResultSet getGeneratedKeys() throws SQLException {
		synchronized (this) {
			checkClosed();
			if (generatedKeys == null || generatedKeys.getResultSet() == null) {
				return createDriverResultSet(new Field[0], new ArrayList<byte[][]>());
			}

			return generatedKeys.getResultSet();
		}
	}

	public CompletableFuture<Integer> executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		if (autoGeneratedKeys == VxBaseStatement.NO_GENERATED_KEYS) {
			return executeUpdate(sql);
		}

		return executeUpdate(sql, (String[]) null);
	}

	public CompletableFuture<Integer> executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		if (columnIndexes == null || columnIndexes.length == 0) {
			return executeUpdate(sql);
		}

		throw new PSQLException(GT.tr("Returning autogenerated keys by column index is not supported."),
				PSQLState.NOT_IMPLEMENTED);
	}

	public CompletableFuture<Integer> executeUpdate(String sql, String[] columnNames) throws SQLException {
		if (columnNames != null && columnNames.length == 0) {
			return executeUpdate(sql);
		}

		wantsGeneratedKeysOnce = true;

		if (!await(executeCachedSql(sql, 0, columnNames))) {
			// no resultset returned. What's a pity!
		}
	
		return CompletableFuture.completedFuture(getUpdateCount());
	}

	public CompletableFuture<Boolean> execute(String sql, int autoGeneratedKeys) throws SQLException {
		if (autoGeneratedKeys == VxBaseStatement.NO_GENERATED_KEYS) {
			return execute(sql);
		}
		return execute(sql, (String[]) null);
	}

	public CompletableFuture<Boolean> execute(String sql, int[] columnIndexes) throws SQLException {
		if (columnIndexes != null && columnIndexes.length == 0) {
			return execute(sql);
		}

		throw new PSQLException(GT.tr("Returning autogenerated keys by column index is not supported."),
				PSQLState.NOT_IMPLEMENTED);
	}

	public CompletableFuture<Boolean> execute(String sql, String[] columnNames) throws SQLException {
		if (columnNames != null && columnNames.length == 0) {
			return execute(sql);
		}

		wantsGeneratedKeysOnce = true;
		return executeCachedSql(sql, 0, columnNames);
		
	}

	public int getResultSetHoldability() throws SQLException {
		return rsHoldability;
	}

	public VxResultSet createDriverResultSet(Field[] fields, List<byte[][]> tuples) throws SQLException {
		return createResultSet(null, fields, tuples, null);
	}

	protected void transformQueriesAndParameters() throws SQLException {
	}

}
