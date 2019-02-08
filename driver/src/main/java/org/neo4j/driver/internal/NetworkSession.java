/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.logging.PrefixedLogger;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.Supplier;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionConfig;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.Collections.emptyMap;
import static org.neo4j.driver.internal.TransactionUtils.ensureNoOpenTxBeforeRunningQuery;
import static org.neo4j.driver.internal.TransactionUtils.ensureNoOpenTxBeforeStartingTx;
import static org.neo4j.driver.internal.TransactionUtils.executeWork;
import static org.neo4j.driver.internal.TransactionUtils.existingTransactionOrNull;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;

public class NetworkSession extends AbstractStatementRunner implements Session
{
    private static final String LOG_NAME = "Session";

    private final ConnectionProvider connectionProvider;
    private final AccessMode mode;
    private final RetryLogic retryLogic;
    protected final Logger logger;

    private final BookmarksHolder bookmarksHolder;
    private volatile CompletionStage<ExplicitTransaction> transactionStage = completedWithNull();
    private volatile CompletionStage<Connection> connectionStage = completedWithNull();
    private volatile CompletionStage<InternalStatementResultCursor> resultCursorStage = completedWithNull();

    private final AtomicBoolean open = new AtomicBoolean( true );

    public NetworkSession( ConnectionProvider connectionProvider, AccessMode mode, RetryLogic retryLogic, Logging logging, BookmarksHolder bookmarksHolder )
    {
        this.connectionProvider = connectionProvider;
        this.mode = mode;
        this.retryLogic = retryLogic;
        this.logger = new PrefixedLogger( "[" + hashCode() + "]", logging.getLog( LOG_NAME ) );
        this.bookmarksHolder = bookmarksHolder;
    }

    @Override
    public StatementResult run( Statement statement )
    {
        return run( statement, TransactionConfig.empty() );
    }

    @Override
    public StatementResult run( String statement, TransactionConfig config )
    {
        return run( statement, emptyMap(), config );
    }

    @Override
    public StatementResult run( String statement, Map<String,Object> parameters, TransactionConfig config )
    {
        return run( new Statement( statement, parameters ), config );
    }

    @Override
    public StatementResult run( Statement statement, TransactionConfig config )
    {
        StatementResultCursor cursor = Futures.blockingGet( run( statement, config, false ),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while running query in session" ) );

        // query executed, it is safe to obtain a connection in a blocking way
        Connection connection = Futures.getNow( connectionStage );
        return new InternalStatementResult( connection, cursor );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( Statement statement )
    {
        return runAsync( statement, TransactionConfig.empty() );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statement, TransactionConfig config )
    {
        return runAsync( statement, emptyMap(), config );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statement, Map<String,Object> parameters, TransactionConfig config )
    {
        return runAsync( new Statement( statement, parameters ), config );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( Statement statement, TransactionConfig config )
    {
        //noinspection unchecked
        return (CompletionStage) run( statement, config, true );
    }

    @Override
    public boolean isOpen()
    {
        return open.get();
    }

    @Override
    public void close()
    {
        Futures.blockingGet( closeAsync(),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while closing the session" ) );
    }

    @Override
    public CompletionStage<Void> closeAsync()
    {
        if ( open.compareAndSet( true, false ) )
        {
            return resultCursorStage.thenCompose( cursor ->
            {
                if ( cursor != null )
                {
                    // there exists a cursor with potentially unconsumed error, try to extract and propagate it
                    return cursor.failureAsync();
                }
                // no result cursor exists so no error exists
                return completedWithNull();
            } ).thenCombine( closeTransactionAndReleaseConnection(), TransactionUtils::combineCursorAndTxCloseError );
        }
        return completedWithNull();
    }

    @Override
    public Transaction beginTransaction()
    {
        return beginTransaction( TransactionConfig.empty() );
    }

    @Override
    public Transaction beginTransaction( TransactionConfig config )
    {
        return beginTransaction( mode, config );
    }

    @Deprecated
    @Override
    public Transaction beginTransaction( String bookmark )
    {
        bookmarksHolder.setBookmarks( Bookmarks.from( bookmark ) );
        return beginTransaction();
    }

    @Override
    public CompletionStage<Transaction> beginTransactionAsync()
    {
        return beginTransactionAsync( TransactionConfig.empty() );
    }

    @Override
    public CompletionStage<Transaction> beginTransactionAsync( TransactionConfig config )
    {
        //noinspection unchecked
        return (CompletionStage) beginTransactionAsync( mode, config );
    }

    @Override
    public <T> T readTransaction( TransactionWork<T> work )
    {
        return readTransaction( work, TransactionConfig.empty() );
    }

    @Override
    public <T> T readTransaction( TransactionWork<T> work, TransactionConfig config )
    {
        return transaction( AccessMode.READ, work, config );
    }

    @Override
    public <T> CompletionStage<T> readTransactionAsync( TransactionWork<CompletionStage<T>> work )
    {
        return readTransactionAsync( work, TransactionConfig.empty() );
    }

    @Override
    public <T> CompletionStage<T> readTransactionAsync( TransactionWork<CompletionStage<T>> work, TransactionConfig config )
    {
        return transactionAsync( AccessMode.READ, work, config );
    }

    @Override
    public <T> T writeTransaction( TransactionWork<T> work )
    {
        return writeTransaction( work, TransactionConfig.empty() );
    }

    @Override
    public <T> T writeTransaction( TransactionWork<T> work, TransactionConfig config )
    {
        return transaction( AccessMode.WRITE, work, config );
    }

    @Override
    public <T> CompletionStage<T> writeTransactionAsync( TransactionWork<CompletionStage<T>> work )
    {
        return writeTransactionAsync( work, TransactionConfig.empty() );
    }

    @Override
    public <T> CompletionStage<T> writeTransactionAsync( TransactionWork<CompletionStage<T>> work, TransactionConfig config )
    {
        return transactionAsync( AccessMode.WRITE, work, config );
    }

    @Override
    public String lastBookmark()
    {
        return bookmarksHolder.lastBookmark();
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public void reset()
    {
        Futures.blockingGet( resetAsync(),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while resetting the session" ) );
    }

    private CompletionStage<Void> resetAsync()
    {
        return existingTransactionOrNull(transactionStage)
                .thenAccept( tx ->
                {
                    if ( tx != null )
                    {
                        tx.markTerminated();
                    }
                } )
                .thenCompose( ignore -> connectionStage )
                .thenCompose( connection ->
                {
                    if ( connection != null )
                    {
                        // there exists an active connection, send a RESET message over it
                        return connection.reset();
                    }
                    return completedWithNull();
                } );
    }

    CompletionStage<Boolean> currentConnectionIsOpen()
    {
        return connectionStage.handle( ( connection, error ) ->
                error == null && // no acquisition error
                connection != null && // some connection has actually been acquired
                connection.isOpen() ); // and it's still open
    }

    private <T> T transaction( AccessMode mode, TransactionWork<T> work, TransactionConfig config )
    {
        // use different code path compared to async so that work is executed in the caller thread
        // caller thread will also be the one who sleeps between retries;
        // it is unsafe to execute retries in the event loop threads because this can cause a deadlock
        // event loop thread will bock and wait for itself to read some data

        Supplier<Transaction> transactionSupplier = () -> beginTransaction( mode, config );
        return retryLogic.retry( () -> executeWork( transactionSupplier, work ) );
    }

    private <T> CompletionStage<T> transactionAsync( AccessMode mode, TransactionWork<CompletionStage<T>> work, TransactionConfig config )
    {
        return retryLogic.retryAsync( () ->
        {
            CompletableFuture<T> resultFuture = new CompletableFuture<>();
            CompletionStage<ExplicitTransaction> txFuture = beginTransactionAsync( mode, config );

            txFuture.whenComplete( ( tx, completionError ) ->
            {
                Throwable error = Futures.completionExceptionCause( completionError );
                if ( error != null )
                {
                    resultFuture.completeExceptionally( error );
                }
                else
                {
                    executeWork( resultFuture, tx, work );
                }
            } );

            return resultFuture;
        } );
    }

    private CompletionStage<InternalStatementResultCursor> run( Statement statement, TransactionConfig config, boolean waitForRunResponse )
    {
        ensureSessionIsOpen();

        CompletionStage<InternalStatementResultCursor> newResultCursorStage = ensureNoOpenTxBeforeRunningQuery(transactionStage)
                .thenCompose( ignore -> acquireConnection( mode ) )
                .thenCompose( connection ->
                        connection.protocol().runInAutoCommitTransaction( connection, statement, bookmarksHolder, config, waitForRunResponse ) );

        resultCursorStage = newResultCursorStage.exceptionally( error -> null );

        return newResultCursorStage;
    }

    private Transaction beginTransaction( AccessMode mode, TransactionConfig config )
    {
        return Futures.blockingGet( beginTransactionAsync( mode, config ),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while starting a transaction" ) );
    }

    private CompletionStage<ExplicitTransaction> beginTransactionAsync( AccessMode mode, TransactionConfig config )
    {
        ensureSessionIsOpen();

        // create a chain that acquires connection and starts a transaction
        CompletionStage<ExplicitTransaction> newTransactionStage = ensureNoOpenTxBeforeStartingTx(transactionStage)
                .thenCompose( ignore -> acquireConnection( mode ) )
                .thenCompose( connection ->
                {
                    ExplicitTransaction tx = new ExplicitTransaction( connection, bookmarksHolder );
                    return tx.beginAsync( bookmarksHolder.getBookmarks(), config );
                } );

        transactionStage = newTransactionStage
                // ignore errors from starting new transaction
                .exceptionally( error -> null )
                // update the reference to the only known transaction
                .thenCompose( TransactionUtils.newOrCurrentTransaction( transactionStage ) );
        return newTransactionStage;
    }

    private CompletionStage<Connection> acquireConnection( AccessMode mode )
    {
        CompletionStage<Connection> currentConnectionStage = connectionStage;

        CompletionStage<Connection> newConnectionStage = resultCursorStage.thenCompose( cursor ->
        {
            if ( cursor == null )
            {
                return completedWithNull();
            }
            // make sure previous result is fully consumed and connection is released back to the pool
            return cursor.failureAsync();
        } ).thenCompose( error ->
        {
            if ( error == null )
            {
                // there is no unconsumed error, so one of the following is true:
                //   1) this is first time connection is acquired in this session
                //   2) previous result has been successful and is fully consumed
                //   3) previous result failed and error has been consumed

                // return existing connection, which should've been released back to the pool by now
                return currentConnectionStage.exceptionally( ignore -> null );
            }
            else
            {
                // there exists unconsumed error, re-throw it
                throw new CompletionException( error );
            }
        } ).thenCompose( existingConnection ->
        {
            if ( existingConnection != null && existingConnection.isOpen() )
            {
                // there somehow is an existing open connection, this should not happen, just a precondition
                throw new IllegalStateException( "Existing open connection detected" );
            }
            return connectionProvider.acquireConnection( mode );
        } );

        connectionStage = newConnectionStage.exceptionally( error -> null );

        return newConnectionStage;
    }

    private CompletionStage<Throwable> closeTransactionAndReleaseConnection()
    {
        return existingTransactionOrNull(transactionStage).thenCompose( tx ->
        {
            if ( tx != null )
            {
                // there exists an open transaction, let's close it and propagate the error, if any
                return tx.closeAsync()
                        .thenApply( ignore -> (Throwable) null )
                        .exceptionally( error -> error );
            }
            // no open transaction so nothing to close
            return completedWithNull();
        } ).thenCompose( txCloseError ->
                // then release the connection and propagate transaction close error, if any
                releaseConnection().thenApply( ignore -> txCloseError ) );
    }

    private CompletionStage<Void> releaseConnection()
    {
        return connectionStage.thenCompose( connection ->
        {
            if ( connection != null )
            {
                // there exists connection, try to release it back to the pool
                return connection.release();
            }
            // no connection so return null
            return completedWithNull();
        } );
    }

    private void terminateConnectionOnThreadInterrupt( String reason )
    {
        // try to get current connection if it has been acquired
        Connection connection = null;
        try
        {
            connection = Futures.getNow( connectionStage );
        }
        catch ( Throwable ignore )
        {
            // ignore errors because handing interruptions is best effort
        }

        if ( connection != null )
        {
            connection.terminateAndRelease( reason );
        }
    }

    private void ensureSessionIsOpen()
    {
        if ( !open.get() )
        {
            throw new ClientException(
                    "No more interaction with this session are allowed as the current session is already closed. " );
        }
    }
}
