package org.neo4j.driver.internal;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.neo4j.driver.internal.types.InternalMapAccessorWithDefaultValue;
import org.neo4j.driver.internal.util.Extract;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.util.Pair;

import static java.lang.String.format;
import static org.neo4j.driver.internal.util.Format.formatPairs;
import static org.neo4j.driver.v1.Values.ofObject;
import static org.neo4j.driver.v1.Values.ofValue;

abstract class AbstractRecord extends InternalMapAccessorWithDefaultValue implements Record
{
    private final List<String> keys;
    private final Value[] values;

    public AbstractRecord( List<String> keys, Value[] values )
    {
        this.keys = keys;
        this.values = values;
    }

    @Override
    public List<String> keys()
    {
        return keys;
    }

    @Override
    public List<Value> values()
    {
        return Arrays.asList( values );
    }

    @Override
    public List<Pair<String,Value>> fields()
    {
        return Extract.fields( this, ofValue() );
    }

    @Override
    public int index( String key )
    {
        int result = keys.indexOf( key );
        if ( result == -1 )
        {
            throw new NoSuchElementException( "Unknown key: " + key );
        }
        else
        {
            return result;
        }
    }

    @Override
    public boolean containsKey( String key )
    {
        return keys.contains( key );
    }

    @Override
    public Value get( String key )
    {
        int fieldIndex = keys.indexOf( key );

        if ( fieldIndex == -1 )
        {
            return Values.NULL;
        }
        else
        {
            return values[fieldIndex];
        }
    }

    @Override
    public Value get( int index )
    {
        return index >= 0 && index < values.length ? values[index] : Values.NULL;
    }

    @Override
    public int size()
    {
        return values.length;
    }

    @Override
    public Map<String,Object> asMap()
    {
        return Extract.map( this, ofObject() );
    }

    @Override
    public <T> Map<String,T> asMap( Function<Value,T> mapper )
    {
        return Extract.map( this, mapper );
    }

    @Override
    public String toString()
    {
        return format( "Record<%s>", formatPairs( asMap( ofValue() ) ) );
    }

    @Override
    public boolean equals( Object other )
    {
        if ( this == other )
        {
            return true;
        }
        else if ( other instanceof Record )
        {
            Record otherRecord = (Record) other;
            int size = size();
            if ( !(size == otherRecord.size()) )
            {
                return false;
            }
            if ( !keys.equals( otherRecord.keys() ) )
            {
                return false;
            }
            for ( int i = 0; i < size; i++ )
            {
                Value value = get( i );
                Value otherValue = otherRecord.get( i );
                if ( !value.equals( otherValue ) )
                {
                    return false;
                }
            }
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash( keys );
        result = 31 * result + Arrays.hashCode( values );
        return result;
    }
}
