package org.neo4j.driver.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;

public class EmbeddedRecord extends AbstractRecord implements Record
{

    public static EmbeddedRecord of( Map<String,Object> internalRecord )
    {
        List<String> keys = new ArrayList<>( internalRecord.keySet() );
        Value[] values = internalRecord.values().stream().toArray( Value[]::new );

        return new EmbeddedRecord( keys, values );
    }

    private EmbeddedRecord( List<String> keys, Value[] values )
    {
        super( keys, values );
    }
}
