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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.StatementResultCursor;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.graphdb.QueryExecutionException;

import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

/**
 * @since 2.0
 */
public class EmbeddedTransaction extends AbstractTransaction implements Transaction
{

    private final EmbeddedCypherRunner cypherRunner;
    private final ThreadLocal<org.neo4j.graphdb.Transaction> threadBoundTransaction;

    public EmbeddedTransaction( EmbeddedCypherRunner cypherRunner, Supplier<org.neo4j.graphdb.Transaction> transactionSupplier )
    {
        this.cypherRunner = cypherRunner;
        this.threadBoundTransaction = ThreadLocal.withInitial( transactionSupplier );
    }

    @Override
    public void close()
    {
        Futures.blockingGet( closeAsync() );
    }

    @Override
    public StatementResult run( Statement statement )
    {
        try
        {
            StatementResult result = new EmbeddedStatementResult( statement, cypherRunner.execute( statement.text(), statement.parameters().asMap() ) );
            this.success();
            return result;
        }
        catch ( QueryExecutionException e )
        {
            this.failure();
            throw e;
        }
        finally
        {
            this.close();
        }
    }

    @Override
    public CompletionStage<StatementResultCursor> runAsync( Statement statement )
    {
        return null;
    }

    @Override
    protected final CompletionStage<Void> doCommitAsync()
    {
        if ( state == TransactionState.TERMINATED )
        {
            return failedFuture(
                    new ClientException( "Transaction can't be committed. " + "It has been rolled back either because of an error or explicit termination" ) );
        }

        return CompletableFuture.runAsync( () ->
        {
            org.neo4j.graphdb.Transaction transaction = this.threadBoundTransaction.get();
            transaction.success();
            transaction.close();
        } );
    }

    @Override
    protected final CompletionStage<Void> doRollbackAsync()
    {
        if ( state == TransactionState.TERMINATED )
        {
            return completedWithNull();
        }
        return CompletableFuture.runAsync( () ->
        {
            org.neo4j.graphdb.Transaction transaction = this.threadBoundTransaction.get();
            transaction.failure();
            transaction.close();
        } );
    }

    @Override
    protected final void transactionClosed( TransactionState newState )
    {
        state = newState;
    }
}
