package com.alandevise.tsgate.adapter.impl;

import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.tsfile.write.record.Tablet;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * An IoTDB table-session pool with a stable public identity and replaceable physical pool versions.
 * <p>Spring can retain the same proxy. Replacing the pool stops new borrows from the old version,
 * which closes after all its borrowed sessions return, without interrupting concurrent reads or writes.</p>
 * @author Alan Zhang [initiator@alandevise.com]
 * @since 2026-08-26
 */
final class RecoverableTableSessionPool implements ITableSessionPool {

    private PoolVersion current;
    private long version;
    private boolean closed;

    /**
     * Create a stable proxy around the initial official client pool.
     * @param initialPool initial official IoTDB pool
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-26
     */
    RecoverableTableSessionPool(ITableSessionPool initialPool) {
        current = new PoolVersion(Objects.requireNonNull(initialPool, "initialPool"));
    }

    /**
     * Return the current physical pool version for conditional concurrent recovery.
     * @return current physical pool version
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-26
     */
    synchronized long currentVersion() {
        return version;
    }

    /**
     * Replace the pool only if the failed request used the current version, avoiding duplicate concurrent recovery.
     * @param expectedVersion physical pool version used by the failed request
     * @param poolFactory factory for the replacement physical pool
     * @return whether this thread replaced the pool
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-26
     */
    boolean replaceIfCurrent(long expectedVersion,
                             Supplier<ITableSessionPool> poolFactory) {
        Objects.requireNonNull(poolFactory, "poolFactory");
        synchronized (this) {
            if (closed || version != expectedVersion) {
                return false;
            }
        }

        ITableSessionPool replacement = Objects.requireNonNull(poolFactory.get(), "replacementPool");
        PoolVersion retired;
        synchronized (this) {
            if (closed || version != expectedVersion) {
                replacement.close();
                return false;
            }
            retired = current;
            current = new PoolVersion(replacement);
            version++;
        }
        retired.retire();
        return true;
    }

    /**
     * Borrow a table session that tracks the physical pool version to which it must return.
     * @return tracked session that returns to its owning physical pool version
     * @throws IllegalStateException if the pool is closed
     * @throws IoTDBConnectionException if a connection fails while performing the operation or releasing resources
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-26
     */
    @Override
    public ITableSession getSession() throws IoTDBConnectionException {
        return borrowSession().session();
    }

    /**
     * Return the borrowed session together with its actual physical pool version so recovery can replace the failed version precisely.
     * @return borrow result containing the physical pool version and tracked session
     * @throws IllegalStateException if the pool is closed
     * @throws IoTDBConnectionException if a connection fails while performing the operation or releasing resources
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-26
     */
    BorrowedTableSession borrowSession() throws IoTDBConnectionException {
        PoolVersion selected;
        long selectedVersion;
        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("IoTDB table SessionPool is closed");
            }
            selected = current;
            selectedVersion = version;
            selected.acquire();
        }

        boolean borrowed = false;
        try {
            BorrowedTableSession borrowedSession = new BorrowedTableSession(
                    selectedVersion,
                    new TrackedTableSession(selected.pool.getSession(), selected));
            borrowed = true;
            return borrowedSession;
        } finally {
            if (!borrowed) {
                selected.release();
            }
        }
    }

    /**
     * The actual pool version and tracked session used to identify a failed pool during recovery.
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-26
     */
    static final class BorrowedTableSession {
        private final long version;
        private final ITableSession session;

        /**
         * Create a borrowed-session result with its physical pool version.
         * @param version physical pool version associated with the borrowed session
         * @param session tracked table-model session
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-08-26
         */
        private BorrowedTableSession(long version,
                                     ITableSession session) {
            this.version = version;
            this.session = session;
        }

        /**
         * Return the physical pool version from which the session was borrowed.
         * @return physical pool version
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-08-26
         */
        long version() {
            return version;
        }

        /**
         * Return the table session that tracks its owning pool version.
         * @return tracked table-model session
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-08-26
         */
        ITableSession session() {
            return session;
        }
    }

    /**
     * Stop accepting new borrows and close the physical pool after all sessions in its current version return.
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-26
     */
    @Override
    public void close() {
        PoolVersion retired;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            retired = current;
            current = null;
        }
        retired.retire();
    }

    /**
     * A physical pool version and its outstanding borrowed-session count.
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-26
     */
    private static final class PoolVersion {
        private final ITableSessionPool pool;
        private int borrowedSessionCount;
        private boolean retired;
        private boolean poolClosed;

        /**
         * Create the state for a physical pool version.
         * @param pool official IoTDB physical pool
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-08-26
         */
        private PoolVersion(ITableSessionPool pool) {
            this.pool = pool;
        }

        /**
         * Record a newly borrowed session for this version.
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-08-26
         */
        private synchronized void acquire() {
            if (retired) {
                throw new IllegalStateException("Cannot borrow a session from a retired IoTDB pool version");
            }
            borrowedSessionCount++;
        }

        /**
         * Record a returned session and close the retired physical pool once no borrowed sessions remain.
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-08-26
         */
        private void release() {
            ITableSessionPool toClose = null;
            synchronized (this) {
                if (borrowedSessionCount <= 0) {
                    return;
                }
                borrowedSessionCount--;
                if (retired && borrowedSessionCount == 0 && !poolClosed) {
                    poolClosed = true;
                    toClose = pool;
                }
            }
            closePool(toClose);
        }

        /**
         * Retire this version from new borrows and close its physical pool after all borrowed sessions return.
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-08-26
         */
        private void retire() {
            ITableSessionPool toClose = null;
            synchronized (this) {
                retired = true;
                if (borrowedSessionCount == 0 && !poolClosed) {
                    poolClosed = true;
                    toClose = pool;
                }
            }
            closePool(toClose);
        }

        /**
         * Close the official IoTDB pool if present.
         * @param pool official IoTDB physical pool
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-08-26
         */
        private static void closePool(ITableSessionPool pool) {
            if (pool != null) {
                pool.close();
            }
        }
    }

    /**
     * Track a session's physical pool version; returning the session also updates that version's borrowed-session count.
     * @author Alan Zhang [initiator@alandevise.com]
     * @since 2026-08-26
     */
    private static final class TrackedTableSession implements ITableSession {
        private final ITableSession delegate;
        private final PoolVersion poolVersion;
        private final AtomicBoolean closed = new AtomicBoolean();

        /**
         * Create a table session that tracks its physical pool version.
         * @param delegate physical session borrowed from the official pool
         * @param poolVersion physical pool version that owns the session
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-08-26
         */
        private TrackedTableSession(ITableSession delegate,
                                    PoolVersion poolVersion) {
            this.delegate = delegate;
            this.poolVersion = poolVersion;
        }

        /**
         * Insert a batch of table-model data into IoTDB.
         * @param tablet assembled table-model data batch
         * @throws StatementExecutionException if the server fails to execute the operation or release its result
         * @throws IoTDBConnectionException if a connection fails while performing the operation or releasing resources
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-08-26
         */
        @Override
        public void insert(Tablet tablet)
                throws StatementExecutionException, IoTDBConnectionException {
            delegate.insert(tablet);
        }

        /**
         * Execute a statement that does not return a result set.
         * @param sql IoTDB table-model SQL statement
         * @throws IoTDBConnectionException if a connection fails while performing the operation or releasing resources
         * @throws StatementExecutionException if the server fails to execute the operation or release its result
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-08-26
         */
        @Override
        public void executeNonQueryStatement(String sql)
                throws IoTDBConnectionException, StatementExecutionException {
            delegate.executeNonQueryStatement(sql);
        }

        /**
         * Execute a query and return its result set.
         * @param sql IoTDB table-model SQL statement
         * @return query result set
         * @throws StatementExecutionException if the server fails to execute the operation or release its result
         * @throws IoTDBConnectionException if a connection fails while performing the operation or releasing resources
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-08-26
         */
        @Override
        public SessionDataSet executeQueryStatement(String sql)
                throws StatementExecutionException, IoTDBConnectionException {
            return delegate.executeQueryStatement(sql);
        }

        /**
         * Execute a query with the specified timeout and return its result set.
         * @param sql IoTDB table-model SQL statement
         * @param timeoutInMs query timeout in milliseconds
         * @return query result set
         * @throws StatementExecutionException if the server fails to execute the operation or release its result
         * @throws IoTDBConnectionException if a connection fails while performing the operation or releasing resources
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-08-26
         */
        @Override
        public SessionDataSet executeQueryStatement(String sql,
                                                    long timeoutInMs)
                throws StatementExecutionException, IoTDBConnectionException {
            return delegate.executeQueryStatement(sql, timeoutInMs);
        }

        /**
         * Return the physical session and update the owning pool version's borrowed-session count.
         * @throws IoTDBConnectionException if a connection fails while performing the operation or releasing resources
         * @author Alan Zhang [initiator@alandevise.com]
         * @since 2026-08-26
         */
        @Override
        public void close() throws IoTDBConnectionException {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            try {
                delegate.close();
            } finally {
                poolVersion.release();
            }
        }
    }
}
