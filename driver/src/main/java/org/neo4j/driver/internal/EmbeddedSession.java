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

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.logging.PrefixedLogger;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.util.Futures;
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
import org.neo4j.graphdb.GraphDatabaseService;

import static java.util.Collections.emptyMap;
import static org.neo4j.driver.internal.TransactionUtils.ensureNoOpenTxBeforeRunningQuery;
import static org.neo4j.driver.internal.TransactionUtils.ensureNoOpenTxBeforeStartingTx;
import static org.neo4j.driver.internal.TransactionUtils.executeWork;
import static org.neo4j.driver.internal.TransactionUtils.existingTransactionOrNull;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;

/**
 * Session connected to an embedded Neo4j instance in the same VM.
 *
 * @since 2.0
 */
public class EmbeddedSession extends AbstractStatementRunner implements Session
{
    private static final String LOG_NAME = "EmbeddedSession";
    private static final String BOOKMARKS_NOT_SUPPORTED_MESSAGE = "Embedded session does not support bookmarks";

    private final GraphDatabaseService graphDatabaseService;
    private final RetryLogic retryLogic;
    protected final Logger logger;

    private volatile CompletionStage<Transaction> transactionStage = completedWithNull();
    private volatile CompletionStage<InternalStatementResultCursor> resultCursorStage = completedWithNull();

    private final AtomicBoolean open = new AtomicBoolean( true );

    public EmbeddedSession( GraphDatabaseService graphDatabaseService, RetryLogic retryLogic, Logging logging )
    {
        this.graphDatabaseService = graphDatabaseService;
        this.retryLogic = retryLogic;
        this.logger = new PrefixedLogger( "[" + hashCode() + "]", logging.getLog( LOG_NAME ) );
    }

    @Override
    public Transaction beginTransaction()
    {
        return beginTransaction( TransactionConfig.empty() );
    }

    @Override
    public Transaction beginTransaction( TransactionConfig config )
    {
        return Futures.blockingGet( beginTransactionAsync( config ) );
    }

    @Deprecated
    @Override
    public Transaction beginTransaction( String bookmark )
    {
        throw new UnsupportedOperationException( BOOKMARKS_NOT_SUPPORTED_MESSAGE );
    }

    @Override
    public CompletionStage<Transaction> beginTransactionAsync()
    {
        return beginTransactionAsync( TransactionConfig.empty() );
    }

    @Override
    public CompletionStage<Transaction> beginTransactionAsync( TransactionConfig config )
    {
        Objects.requireNonNull( config, "Transaction config can't be null" );

        ensureSessionIsOpen();

        // create a chain that acquires connection and starts a transaction
        CompletionStage<Transaction> newTransactionStage = ensureNoOpenTxBeforeStartingTx( transactionStage ).thenApply( ignore ->
        {
            Duration timeout = config.timeout();
            org.neo4j.graphdb.Transaction transaction = graphDatabaseService.beginTx( timeout.toMillis(), TimeUnit.MILLISECONDS );
            return new EmbeddedTransaction( new DefaultEmbeddedCypherRunner( graphDatabaseService ), transaction );
        } );

        transactionStage = newTransactionStage
                // ignore errors from starting new transaction
                .exceptionally( error -> null )
                // update the reference to the only known transaction
                .thenCompose( TransactionUtils.newOrCurrentTransaction( transactionStage ) );

        return newTransactionStage;
    }

    @Override
    public <T> T readTransaction( TransactionWork<T> work )
    {
        return readTransaction( work, TransactionConfig.empty() );
    }

    @Override
    public <T> T readTransaction( TransactionWork<T> work, TransactionConfig config )
    {
        return retryLogic.retry( () -> executeWork( () -> beginTransaction( config ), work ) );
    }

    @Override
    public <T> CompletionStage<T> readTransactionAsync( TransactionWork<CompletionStage<T>> work )
    {
        return readTransactionAsync( work, TransactionConfig.empty() );
    }

    @Override
    public <T> CompletionStage<T> readTransactionAsync( TransactionWork<CompletionStage<T>> work, TransactionConfig config )
    {
        return null;
    }

    @Override
    public <T> T writeTransaction( TransactionWork<T> work )
    {
        return writeTransaction( work, TransactionConfig.empty() );
    }

    @Override
    public <T> T writeTransaction( TransactionWork<T> work, TransactionConfig config )
    {
        return retryLogic.retry( () -> executeWork( () -> beginTransaction( config ), work ) );
    }

    @Override
    public <T> CompletionStage<T> writeTransactionAsync( TransactionWork<CompletionStage<T>> work )
    {
        return writeTransactionAsync( work, TransactionConfig.empty() );
    }

    @Override
    public <T> CompletionStage<T> writeTransactionAsync( TransactionWork<CompletionStage<T>> work, TransactionConfig config )
    {
        return null;
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
        ensureSessionIsOpen();
        return Futures.blockingGet( ensureNoOpenTxBeforeRunningQuery( transactionStage ).thenApply( __ -> new EmbeddedStatementResult(null) ) );
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
        return null;
    }

    @Override
    public String lastBookmark()
    {
        throw new UnsupportedOperationException( BOOKMARKS_NOT_SUPPORTED_MESSAGE );
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public void reset()
    {
        Futures.blockingGet( resetAsync() );
    }

    private CompletionStage<Void> resetAsync()
    {
        return existingTransactionOrNull( transactionStage ).thenAccept( tx ->
        {
            if ( tx != null )
            {
                if ( tx instanceof AbstractTransaction )
                {
                    ((AbstractTransaction) tx).markTerminated();
                }
            }
        } );
    }

    @Override
    public boolean isOpen()
    {
        return open.get();
    }

    @Override
    public void close()
    {
        Futures.blockingGet( closeAsync() );
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
            } ).thenCombine( closeTransaction(), TransactionUtils::combineCursorAndTxCloseError );
        }
        return completedWithNull();
    }

    private CompletionStage<Throwable> closeTransaction()
    {
        return existingTransactionOrNull( transactionStage ).thenCompose( tx ->
        {
            if ( tx != null )
            {
                CompletionStage<Void> closingStage;
                if ( tx instanceof AbstractTransaction )
                {
                    closingStage = ((AbstractTransaction) tx).closeAsync();
                }
                else
                {
                    closingStage = CompletableFuture.runAsync( () -> tx.close() );
                }

                // there exists an open transaction, let's close it and propagate the error, if any
                return closingStage.thenApply( ignore -> (Throwable) null ).exceptionally( error -> error );
            }
            // no open transaction so nothing to close
            return completedWithNull();
        } );
    }

    @Override
    public StatementResult run( Statement statement )
    {
        return run( statement, TransactionConfig.empty() );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( Statement statement )
    {
        return runAsync( statement, TransactionConfig.empty() );
    }

    private void ensureSessionIsOpen()
    {
        if ( !open.get() )
        {
            throw new ClientException( "No more interaction with this session are allowed as the current session is already closed. " );
        }
    }
}
