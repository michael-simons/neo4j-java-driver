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
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;

import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

/**
 * @since 2.0
 */
abstract class AbstractTransaction extends AbstractStatementRunner implements Transaction
{
    protected final ResultCursorsHolder resultCursors = new ResultCursorsHolder();
    protected volatile TransactionState state = TransactionState.ACTIVE;

    @Override
    public void success()
    {
        if ( state == TransactionState.ACTIVE )
        {
            state = TransactionState.MARKED_SUCCESS;
        }
    }

    @Override
    public void failure()
    {
        if ( state == TransactionState.ACTIVE || state == TransactionState.MARKED_SUCCESS )
        {
            state = TransactionState.MARKED_FAILED;
        }
    }

    @Override
    public CompletionStage<Void> commitAsync()
    {
        if ( state == TransactionState.COMMITTED )
        {
            return completedWithNull();
        }
        else if ( state == TransactionState.ROLLED_BACK )
        {
            return failedFuture( new ClientException( "Can't commit, transaction has been rolled back" ) );
        }
        else
        {
            return resultCursors.retrieveNotConsumedError()
                    .thenCompose( error -> doCommitAsync().handle( handleCommitOrRollback( error ) ) ).whenComplete(
                            ( ignore, error ) -> transactionClosed( TransactionState.COMMITTED ) );
        }
    }

    private static BiFunction<Void,Throwable,Void> handleCommitOrRollback( Throwable cursorFailure )
    {
        return ( ignore, commitOrRollbackError ) ->
        {
            CompletionException combinedError = Futures.combineErrors( cursorFailure, commitOrRollbackError );
            if ( combinedError != null )
            {
                throw combinedError;
            }
            return null;
        };
    }

    @Override
    public CompletionStage<Void> rollbackAsync()
    {
        if ( state == TransactionState.COMMITTED )
        {
            return failedFuture( new ClientException( "Can't rollback, transaction has been committed" ) );
        }
        else if ( state == TransactionState.ROLLED_BACK )
        {
            return completedWithNull();
        }
        else
        {
            return resultCursors.retrieveNotConsumedError()
                    .thenCompose( error -> doRollbackAsync().handle( handleCommitOrRollback( error ) ) ).whenComplete(
                            ( ignore, error ) -> transactionClosed( TransactionState.ROLLED_BACK ) );
        }
    }

    protected abstract <T> CompletionStage doCommitAsync();

    protected abstract <T> CompletionStage doRollbackAsync();

    protected abstract void transactionClosed( TransactionState rolledBack );

    protected CompletionStage<Void> closeAsync()
    {
        if ( state == TransactionState.MARKED_SUCCESS )
        {
            return commitAsync();
        }
        else if ( state != TransactionState.COMMITTED && state != TransactionState.ROLLED_BACK )
        {
            return rollbackAsync();
        }
        else
        {
            return completedWithNull();
        }
    }

    protected void ensureCanRunQueries()
    {
        if ( state == TransactionState.COMMITTED )
        {
            throw new ClientException( "Cannot run more statements in this transaction, it has been committed" );
        }
        else if ( state == TransactionState.ROLLED_BACK )
        {
            throw new ClientException( "Cannot run more statements in this transaction, it has been rolled back" );
        }
        else if ( state == TransactionState.MARKED_FAILED )
        {
            throw new ClientException(
                    "Cannot run more statements in this transaction, it has been marked for failure. " + "Please either rollback or close this transaction" );
        }
        else if ( state == TransactionState.TERMINATED )
        {
            throw new ClientException(
                    "Cannot run more statements in this transaction, " + "it has either experienced an fatal error or was explicitly terminated" );
        }
    }

    @Override
    public boolean isOpen()
    {
        return state != TransactionState.COMMITTED && state != TransactionState.ROLLED_BACK;
    }

    public void markTerminated()
    {
        state = TransactionState.TERMINATED;
    }
}
