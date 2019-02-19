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
import java.util.Objects;
import java.util.concurrent.CompletionStage;
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
import static org.neo4j.driver.internal.TransactionUtils.executeWork;

/**
 * Session connected to an embedded Neo4j instance in the same VM.
 *
 * @since 2.0
 */
public class EmbeddedSession extends AbstractStatementRunner implements Session
{
    static final String LOG_NAME = "EmbeddedSession";
    static final String BOOKMARKS_NOT_SUPPORTED_MESSAGE = "Embedded session does not support bookmarks";

    private GraphDatabaseService graphDatabaseService;
    private final RetryLogic retryLogic;
    protected final Logger logger;

    private volatile Transaction currentTransaction;

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
        Objects.requireNonNull( config, "Transaction config can't be null" );

        ensureSessionIsOpen();

        Transaction newTransaction = currentTransaction;
        if ( !isUsable( newTransaction ) )
        {
            synchronized ( this )
            {
                newTransaction = currentTransaction;
                if ( !isUsable( newTransaction ) )
                {
                    newTransaction = currentTransaction = EmbeddedTransaction.begin( graphDatabaseService, config, false );
                    return newTransaction;
                }
            }
        }
        throw new ClientException( TransactionUtils.ERROR_MESSAGE_TX_OPEN_BEFORE_NEW_TX );
    }

    private static boolean isUsable( Transaction transaction )
    {
        return transaction != null && transaction.isOpen();
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
    public StatementResult run( Statement statement )
    {
        return run( statement, TransactionConfig.empty() );
    }

    @Override
    public StatementResult run( Statement statement, TransactionConfig config )
    {
        Objects.requireNonNull( statement, "Statement can't be null" );
        Objects.requireNonNull( config, "Transaction config can't be null" );

        ensureSessionIsOpen();

        if(this.currentTransaction != null) {
            new ClientException( TransactionUtils.ERROR_MESSAGE_TX_OPEN_BEFORE_STATEMENT );
        }
        return EmbeddedTransaction.begin( graphDatabaseService, config, true ).run( statement );
    }

    @Override
    @SuppressWarnings( "deprecation" )
    public void reset()
    {
        Futures.blockingGet( resetAsync() );
    }

    @Override
    public boolean isOpen()
    {
        return open.get();
    }

    @Override
    public void close()
    {
        if ( this.currentTransaction != null )
        {
            this.currentTransaction.close();
        }
        this.currentTransaction = null;
        this.graphDatabaseService = null;
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
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletionStage<Transaction> beginTransactionAsync( TransactionConfig config )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> CompletionStage<T> readTransactionAsync( TransactionWork<CompletionStage<T>> work )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> CompletionStage<T> readTransactionAsync( TransactionWork<CompletionStage<T>> work, TransactionConfig config )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> CompletionStage<T> writeTransactionAsync( TransactionWork<CompletionStage<T>> work )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> CompletionStage<T> writeTransactionAsync( TransactionWork<CompletionStage<T>> work, TransactionConfig config )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statement, TransactionConfig config )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( String statement, Map<String,Object> parameters, TransactionConfig config )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( Statement statement, TransactionConfig config )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( Statement statement )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String lastBookmark()
    {
        throw new UnsupportedOperationException( BOOKMARKS_NOT_SUPPORTED_MESSAGE );
    }

    private CompletionStage<Void> resetAsync()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletionStage<Void> closeAsync()
    {
        throw new UnsupportedOperationException();
    }

    private void ensureSessionIsOpen()
    {
        if ( !open.get() )
        {
            throw new ClientException( "No more interaction with this session are allowed as the current session is already closed. " );
        }
    }
}
