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

import java.util.concurrent.CompletionStage;

import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;

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

    private TransactionUtils()
    {
    }
}
