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

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.messaging.ValueUnpacker;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.PointValue;

/**
 * @since 2.0
 */
public class EmbeddedRecord extends AbstractRecord implements Record
{
    public static EmbeddedRecord of( Map<String,Object> internalRecord )
    {
        List<String> keys = new ArrayList<>( internalRecord.keySet() );
        Value[] values = internalRecord.values().stream().map( EmbeddedRecord::mapToValue ).toArray( Value[]::new );

        return new EmbeddedRecord( keys, values );
    }

    /**
     * TODO Will probably end up as something similar to {@link ValueUnpacker}.
     *
     * @param o
     * @return
     */
    private static Value mapToValue( Object o )
    {

        if ( o == null )
        {
            return NullValue.NULL;
        }

        if ( o instanceof Collection )
        {
            Value[] listValues = ((Collection<?>) o).stream().map( EmbeddedRecord::mapToValue ).toArray( Value[]::new );
            return new ListValue( listValues );
        }

        if ( o instanceof PointValue )
        {
            PointValue point = ((PointValue) o);
            List<Double> nativeCoordinate = point.getCoordinate().getCoordinate();
            Value pointValue;
            if ( nativeCoordinate.size() == 2 )
            {
                pointValue = Values.point( point.getCRS().getCode(), nativeCoordinate.get( 0 ), nativeCoordinate.get( 1 ) );
            }
            else if ( nativeCoordinate.size() == 3 )
            {
                pointValue = Values.point( point.getCRS().getCode(), nativeCoordinate.get( 0 ), nativeCoordinate.get( 1 ), nativeCoordinate.get( 2 ) );
            }
            else
            {
                throw new IllegalArgumentException( "Invalid number of coordinate fields: " + nativeCoordinate.size() );
            }
            return pointValue;
        }

        if ( o instanceof DurationValue )
        {
            DurationValue duration = (DurationValue) o;
            return Values.isoDuration( duration.get( ChronoUnit.MONTHS ), duration.get( ChronoUnit.DAYS ), duration.get( ChronoUnit.SECONDS ),
                    (int) duration.get( ChronoUnit.NANOS ) );
        }

        return Values.value( o );
    }

    private EmbeddedRecord( List<String> keys, Value[] values )
    {
        super( keys, values );
    }
}
