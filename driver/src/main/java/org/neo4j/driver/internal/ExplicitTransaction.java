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

import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

import org.neo4j.driver.internal.async.ResultCursorsHolder;
import org.neo4j.driver.internal.messaging.BoltProtocol;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionConfig;
import org.neo4j.driver.v1.exceptions.ClientException;

import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

public class ExplicitTransaction extends AbstractTransaction implements Transaction
{
    private final Connection connection;
    private final BoltProtocol protocol;
    private final BookmarksHolder bookmarksHolder;
    private final ResultCursorsHolder resultCursors;

    public ExplicitTransaction( Connection connection, BookmarksHolder bookmarksHolder )
    {
        this.connection = connection;
        this.protocol = connection.protocol();
        this.bookmarksHolder = bookmarksHolder;
        this.resultCursors = new ResultCursorsHolder();
    }

    public CompletionStage<ExplicitTransaction> beginAsync( Bookmarks initialBookmarks, TransactionConfig config )
    {
        return protocol.beginTransaction( connection, initialBookmarks, config )
                .handle( ( ignore, beginError ) ->
                {
                    if ( beginError != null )
                    {
                        // release connection if begin failed, transaction can't be started
                        connection.release();
                        throw Futures.asCompletionException( beginError );
                    }
                    return this;
                } );
    }

    @Override
    public void close()
    {
        Futures.blockingGet( closeAsync(),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while closing the transaction" ) );
    }

    @Override
    public StatementResult run( Statement statement )
    {
        StatementResultCursor cursor = Futures.blockingGet( run( statement, false ),
                () -> terminateConnectionOnThreadInterrupt( "Thread interrupted while running query in transaction" ) );
        return new InternalStatementResult( connection, cursor );
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( Statement statement )
    {
        //noinspection unchecked
        return (CompletionStage) run( statement, true );
    }

    private CompletionStage<InternalStatementResultCursor> run( Statement statement, boolean waitForRunResponse )
    {
        ensureCanRunQueries();
        CompletionStage<InternalStatementResultCursor> cursorStage = protocol.runInExplicitTransaction( connection, statement, this, waitForRunResponse );
        resultCursors.add( cursorStage );
        return cursorStage;
    }

    @Override
    protected final CompletionStage<Void> doCommitAsync()
    {
        if ( state == TransactionState.TERMINATED )
        {
            return failedFuture( new ClientException( "Transaction can't be committed. " +
                                                      "It has been rolled back either because of an error or explicit termination" ) );
        }
        return protocol.commitTransaction( connection ).thenAccept( bookmarksHolder::setBookmarks );
    }

    @Override
    protected final CompletionStage<Void> doRollbackAsync()
    {
        if ( state == TransactionState.TERMINATED )
        {
            return completedWithNull();
        }
        return protocol.rollbackTransaction( connection );
    }

    protected final void transactionClosed( TransactionState newState )
    {
        state = newState;
        connection.release(); // release in background
    }

    private void terminateConnectionOnThreadInterrupt( String reason )
    {
        connection.terminateAndRelease( reason );
    }
}
