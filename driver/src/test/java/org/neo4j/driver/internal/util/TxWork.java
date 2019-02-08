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
package org.neo4j.driver.internal.util;

import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;

/**
 * Unit of work used in some session tests.
 */
public class TxWork implements TransactionWork<Integer>
{
    final int result;
    final int timesToThrow;
    final Supplier<RuntimeException> errorSupplier;

    int invoked;

    @SuppressWarnings( "unchecked" )
    public TxWork( int result )
    {
        this( result, 0, (Supplier) null );
    }

    public TxWork( int result, int timesToThrow, final RuntimeException error )
    {
        this.result = result;
        this.timesToThrow = timesToThrow;
        this.errorSupplier = () -> error;
    }

    TxWork( int result, int timesToThrow, Supplier<RuntimeException> errorSupplier )
    {
        this.result = result;
        this.timesToThrow = timesToThrow;
        this.errorSupplier = errorSupplier;
    }

    @Override
    public Integer execute( Transaction tx )
    {
        if ( timesToThrow > 0 && invoked++ < timesToThrow )
        {
            throw errorSupplier.get();
        }
        tx.success();
        return result;
    }
}
