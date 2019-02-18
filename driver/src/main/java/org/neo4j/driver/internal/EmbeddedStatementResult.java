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
import java.util.concurrent.atomic.AtomicBoolean;
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
    public static final boolean NOT_PEEKED_AHEAD = false;
    public static final boolean PEEKED_AHEAD = true;
    /**
     * The original statement executed that materialized this resultset.
     */
    private final Statement statement;
    /**
     * The internal result set returned by the embedded graph database.
     */
    private final Result internalResult;

    private volatile ResultSummary resultSummary;

    /**
     * Filled after {@link #peek()} was successfully called.
     */
    private volatile Record recordPeekedAt;

    private final AtomicBoolean peekedAhead = new AtomicBoolean( NOT_PEEKED_AHEAD );

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
        return peekedAhead.get() || internalResult.hasNext();
    }

    @Override
    public Record peek()
    {
        if ( peekedAhead.compareAndSet( NOT_PEEKED_AHEAD, PEEKED_AHEAD ) )
        {
            synchronized ( this )
            {
                recordPeekedAt = nextImpl();
            }
        }

        return recordPeekedAt;
    }

    @Override
    public Record next()
    {
        if ( peekedAhead.compareAndSet( PEEKED_AHEAD, NOT_PEEKED_AHEAD ) )
        {
            return this.recordPeekedAt;
        }
        else
        {
            return nextImpl();
        }
    }

    private Record nextImpl()
    {
        Map<String,Object> next = internalResult.next();
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
        return StreamSupport.stream( Spliterators.spliteratorUnknownSize( this, Spliterator.ORDERED ), NOT_PEEKED_AHEAD );
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
        // Consume everything and throw it away
        this.list();
        ResultSummary resultSummaryAfterConsumption = this.resultSummary;
        if ( resultSummaryAfterConsumption == null )
        {
            synchronized ( this )
            {
                resultSummaryAfterConsumption = this.resultSummary;
                if ( resultSummaryAfterConsumption == null )
                {
                    this.resultSummary = EmbeddedResultSummary.extractSummary( statement, internalResult );
                    resultSummaryAfterConsumption = this.resultSummary;
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
}
