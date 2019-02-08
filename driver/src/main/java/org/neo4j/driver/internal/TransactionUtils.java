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
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.util.Supplier;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;

/**
 * @since 2.0
 */
final class TransactionUtils
{
    static CompletionStage<Void> ensureNoOpenTxBeforeRunningQuery( CompletionStage<? extends Transaction> transactionStage )
    {
        return ensureNoOpenTx( transactionStage, "Statements cannot be run directly on a session with an open transaction; " +
                "either run from within the transaction or use a different session." );
    }

    static CompletionStage<Void> ensureNoOpenTxBeforeStartingTx( CompletionStage<? extends Transaction> transactionStage )
    {
        return ensureNoOpenTx( transactionStage, "You cannot begin a transaction on a session with an open transaction; " +
                "either run from within the transaction or use a different session." );
    }

    static private CompletionStage<Void> ensureNoOpenTx( CompletionStage<? extends Transaction> transactionStage, String errorMessage )
    {
        return existingTransactionOrNull( transactionStage ).thenAccept( tx ->
        {
            if ( tx != null )
            {
                throw new ClientException( errorMessage );
            }
        } );
    }

    static <T extends Transaction> CompletionStage<T> existingTransactionOrNull( CompletionStage<T> transactionStage )
    {
        return transactionStage.exceptionally( error -> null ) // handle previous connection acquisition and tx begin failures
                .thenApply( tx -> tx != null && tx.isOpen() ? tx : null );
    }

    static <T extends Transaction> Function<T,CompletionStage<T>> newOrCurrentTransaction( CompletionStage<T> currentTransactionStage )
    {
        return tx ->
        {
            if ( tx == null )
            {
                return currentTransactionStage;
            }
            return completedFuture( tx );
        };
    }

    static Void combineCursorAndTxCloseError(Throwable cursorError, Throwable txCloseError)
    {
        // now we have cursor error, active transaction has been closed and connection has been released
        // back to the pool; try to propagate cursor and transaction close errors, if any
        CompletionException combinedError = Futures.combineErrors( cursorError, txCloseError );
        if ( combinedError != null )
        {
            throw combinedError;
        }
        return null;
    }

    static <T> T executeWork( Supplier<Transaction> transactionSupplier, TransactionWork<T> work )
    {
        try ( Transaction tx = transactionSupplier.get() )
        {
            try
            {
                T result = work.execute( tx );
                tx.success();
                return result;
            }
            catch ( Throwable t )
            {
                // mark transaction for failure if the given unit of work threw exception
                // this will override any success marks that were made by the unit of work
                tx.failure();
                throw t;
            }
        }
    }

    static <T> void executeWork( CompletableFuture<T> resultFuture, ExplicitTransaction tx, TransactionWork<CompletionStage<T>> work )
    {
        CompletionStage<T> workFuture = safeExecuteWork( tx, work );
        workFuture.whenComplete( ( result, completionError ) ->
        {
            Throwable error = Futures.completionExceptionCause( completionError );
            if ( error != null )
            {
                rollbackTxAfterFailedTransactionWork( tx, resultFuture, error );
            }
            else
            {
                closeTxAfterSucceededTransactionWork( tx, resultFuture, result );
            }
        } );
    }

    static <T> CompletionStage<T> safeExecuteWork( ExplicitTransaction tx, TransactionWork<CompletionStage<T>> work )
    {
        // given work might fail in both async and sync way
        // async failure will result in a failed future being returned
        // sync failure will result in an exception being thrown
        try
        {
            CompletionStage<T> result = work.execute( tx );

            // protect from given transaction function returning null
            return result == null ? completedWithNull() : result;
        }
        catch ( Throwable workError )
        {
            // work threw an exception, wrap it in a future and proceed
            return failedFuture( workError );
        }
    }

    static <T> void rollbackTxAfterFailedTransactionWork( ExplicitTransaction tx, CompletableFuture<T> resultFuture, Throwable error )
    {
        if ( tx.isOpen() )
        {
            tx.rollbackAsync().whenComplete( ( ignore, rollbackError ) ->
            {
                if ( rollbackError != null )
                {
                    error.addSuppressed( rollbackError );
                }
                resultFuture.completeExceptionally( error );
            } );
        }
        else
        {
            resultFuture.completeExceptionally( error );
        }
    }

    static <T> void closeTxAfterSucceededTransactionWork( ExplicitTransaction tx, CompletableFuture<T> resultFuture, T result )
    {
        if ( tx.isOpen() )
        {
            tx.success();
            tx.closeAsync().whenComplete( ( ignore, completionError ) ->
            {
                Throwable commitError = Futures.completionExceptionCause( completionError );
                if ( commitError != null )
                {
                    resultFuture.completeExceptionally( commitError );
                }
                else
                {
                    resultFuture.complete( result );
                }
            } );
        }
        else
        {
            resultFuture.complete( result );
        }
    }

    private TransactionUtils()
    {
    }
}
