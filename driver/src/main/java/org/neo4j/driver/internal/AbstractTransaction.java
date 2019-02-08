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

import static org.neo4j.driver.internal.util.Futures.completedWithNull;

/**
 * @since 2.0
 */
abstract class AbstractTransaction extends AbstractStatementRunner implements Transaction
{
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
