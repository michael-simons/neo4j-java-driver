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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.graphdb.Result;

public class EmbeddedStatementResult implements StatementResult
{
    /**
     * The original statement executed that materialized this resultset.
     */
    private final Statement statement;
    /**
     * The internal result set returned by the embedded graph database.
     */
    private final Result internalResult;

    private volatile ResultSummary resultSummary;

    public AtomicReference<LazyRecordSupplier> nextRecordSupplier = new AtomicReference<>( new LazyRecordSupplier( this::nextImpl ) );

    EmbeddedStatementResult( Statement statement, Result internalResult )
    {
        this.statement = statement;
        this.internalResult = internalResult;
    }

    @Override
    public List<String> keys()
    {
        return internalResult.columns();
    }

    @Override
    public boolean hasNext()
    {
        return internalResult.hasNext();
    }

    @Override
    public Record peek()
    {
        return nextRecordSupplier.get().get();
    }

    @Override
    public Record next()
    {
        return nextRecordSupplier.getAndSet( new LazyRecordSupplier( this::nextImpl ) ).get();
    }

    private Record nextImpl()
    {
        Map<String,Object> next = internalResult.next();
        if ( !internalResult.hasNext() )
        {
            this.internalResult.close();
        }
        return Optional.ofNullable( next ).map( EmbeddedRecord::of ).orElseThrow( () -> new NoSuchRecordException( "No more records" ) );
    }

    @Override
    public Record single() throws NoSuchRecordException
    {
        if ( !hasNext() )
        {
            throw new NoSuchRecordException( "Cannot retrieve a single record, because this result is empty." );
        }
        else
        {
            Record nextRecord = next();
            if ( hasNext() )
            {
                internalResult.close();
                throw new NoSuchRecordException( //
                        "Expected a result with a single record, but this result " + //
                                "contains at least one more. Ensure your query returns only " + //
                                "one record." );
            }
            return nextRecord;
        }
    }

    @Override
    public Stream<Record> stream()
    {
        return StreamSupport.stream( Spliterators.spliteratorUnknownSize( this, Spliterator.ORDERED ), false );
    }

    @Override
    public List<Record> list()
    {
        return list( r -> r );
    }

    @Override
    public <T> List<T> list( Function<Record,T> mapFunction )
    {
        return this.stream().map( r -> mapFunction.apply( r ) ).collect( Collectors.toList() );
    }

    @Override
    public ResultSummary consume()
    {
        ResultSummary resultSummaryAfterConsumption = resultSummary;
        if ( resultSummaryAfterConsumption == null )
        {
            synchronized ( this )
            {
                resultSummaryAfterConsumption = resultSummary;
                if ( resultSummaryAfterConsumption == null )
                {
                    // Consume everything and throw it away
                    list();
                    resultSummary = EmbeddedResultSummary.extractSummary( statement, internalResult );
                    resultSummaryAfterConsumption = resultSummary;
                }
            }
        }
        return resultSummaryAfterConsumption;
    }

    @Override
    public ResultSummary summary()
    {
        return consume();
    }

    static class LazyRecordSupplier implements Supplier<Record>
    {
        private final Supplier<Record> delegate;
        private volatile Record reference;

        public LazyRecordSupplier( Supplier<Record> delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public Record get()
        {
            Record resolvedReference = reference;
            if ( resolvedReference == null )
            {
                synchronized ( this )
                {
                    resolvedReference = reference;
                    if ( resolvedReference == null )
                    {
                        reference = delegate.get();
                        resolvedReference = reference;
                    }
                }
            }
            return resolvedReference;
        }
    }
}
