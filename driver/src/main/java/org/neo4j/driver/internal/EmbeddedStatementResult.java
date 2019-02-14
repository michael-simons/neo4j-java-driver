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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.exceptions.NoSuchRecordException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.graphdb.Result;

public class EmbeddedStatementResult implements StatementResult
{
    private final Result internalResult;

    public EmbeddedStatementResult( Result internalResult )
    {
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
    public Record next()
    {
        Map<String,Object> next = internalResult.next();
        return Optional.ofNullable( next ).map( EmbeddedRecord::of ).orElseThrow( () -> new NoSuchRecordException( "No more records" ) );
    }

    @Override
    public Record single() throws NoSuchRecordException
    {
        if ( !internalResult.hasNext() )
        {
            throw new NoSuchRecordException( "Cannot retrieve a single record, because this result is empty." );
        }
        else
        {
            Map<String,Object> nextRecord = internalResult.next();
            if ( internalResult.hasNext() )
            {
                internalResult.close();
                throw new NoSuchRecordException( //
                        "Expected a result with a single record, but this result " + //
                                "contains at least one more. Ensure your query returns only " + //
                                "one record." );
            }
            return EmbeddedRecord.of( nextRecord );
        }
    }

    @Override
    public Record peek()
    {
        return null;
    }

    @Override
    public Stream<Record> stream()
    {
        return StreamSupport.stream( Spliterators.spliteratorUnknownSize( this, Spliterator.ORDERED ), false );
    }

    @Override
    public List<Record> list()
    {
        return this.stream().collect( Collectors.toList() );
    }

    @Override
    public <T> List<T> list( Function<Record,T> mapFunction )
    {
        return null;
    }

    @Override
    public ResultSummary consume()
    {
        return null;
    }

    @Override
    public ResultSummary summary()
    {
        return null;
    }
}
