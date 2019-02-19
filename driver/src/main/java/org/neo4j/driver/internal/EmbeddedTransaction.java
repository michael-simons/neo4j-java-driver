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
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionConfig;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;

/**
 * @since 2.0
 */
public class EmbeddedTransaction extends AbstractStatementRunner implements Transaction
{
    private EmbeddedCypherRunner embeddedCypherRunner;
    private boolean open = true;
    private boolean markedAsSuccess = false;
    private final org.neo4j.graphdb.Transaction internalTransaction;
    private Runnable autoCommitHandler;

    static Transaction begin( GraphDatabaseService graphDatabaseService, TransactionConfig config, boolean autoCommit )
    {
        // Use Neo4j's default timeout https://neo4j.com/docs/operations-manual/current/monitoring/transaction-management/#transaction-management-transaction-timeout
        long timeoutInMillis = Optional.ofNullable( config.timeout() ).map( Duration::toMillis ).orElse( 0L );
        return new EmbeddedTransaction( graphDatabaseService, graphDatabaseService.beginTx( timeoutInMillis, TimeUnit.MILLISECONDS ), autoCommit );
    }

    private EmbeddedTransaction( GraphDatabaseService graphDatabaseService, org.neo4j.graphdb.Transaction internalTransaction, boolean autoCommit )
    {
        this.embeddedCypherRunner = EmbeddedCypherRunner.createRunner( graphDatabaseService );
        this.internalTransaction = internalTransaction;
        this.autoCommitHandler = autoCommit ? () -> this.close() : () -> {};
    }

    @Override
    public void success()
    {
        if ( isOpen() )
        {
            this.markedAsSuccess = true;
            this.internalTransaction.success();
        }
    }

    @Override
    public void failure()
    {
        if ( isOpen() )
        {
            this.internalTransaction.failure();
        }
    }

    @Override
    public boolean isOpen()
    {
        return open;
    }

    @Override
    public void close()
    {
        if ( isOpen() )
        {
            if ( !markedAsSuccess )
            {
                this.internalTransaction.failure();
            }
            this.open = false;
            this.internalTransaction.close();
            this.embeddedCypherRunner = null;
        }
    }

    @Override
    public StatementResult run( Statement statement )
    {
        try
        {
            StatementResult result = new EmbeddedStatementResult( autoCommitHandler, statement,
                    embeddedCypherRunner.execute( statement.text(), statement.parameters().asMap() ) );
            this.success();
            return result;
        }
        catch ( QueryExecutionException e )
        {
            this.failure();
            this.autoCommitHandler.run();
            throw e;
        }
    }

    @Override
    public CompletionStage<Void> commitAsync()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletionStage<Void> rollbackAsync()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( Statement statement )
    {
        throw new UnsupportedOperationException();
    }
}
