package org.neo4j.driver.internal;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Values;

/**
 * Simple facade around the {@link GraphDatabaseService} to prevent leaking the methods all over the {@code EmbeddedXXX}-classes.
 *
 * TODO Name is work in progress.
 *
 * @since 2.0
 */
interface EmbeddedCypherRunner
{
    Result execute( String query, Map<String,Object> parameters ) throws QueryExecutionException;
}

class DefaultEmbeddedCypherRunner implements EmbeddedCypherRunner
{
    private final GraphDatabaseService graphDatabaseService;

    public DefaultEmbeddedCypherRunner( GraphDatabaseService graphDatabaseService )
    {
        this.graphDatabaseService = graphDatabaseService;
    }

    public Result execute( String query ) throws QueryExecutionException
    {
        return graphDatabaseService.execute( query );
    }

    public Result execute( String query, long timeout, TimeUnit unit ) throws QueryExecutionException
    {
        return graphDatabaseService.execute( query, timeout, unit );
    }

    public Result execute( String query, Map<String,Object> parameters ) throws QueryExecutionException
    {
        return graphDatabaseService.execute( query, convertParameters( parameters ) );
    }

    public Result execute( String query, Map<String,Object> parameters, long timeout, TimeUnit unit ) throws QueryExecutionException
    {
        return graphDatabaseService.execute( query, convertParameters( parameters ), timeout, unit );
    }

    private static Map<String,Object> convertParameters( Map<String,Object> originalParameters )
    {

        if ( originalParameters == null || originalParameters.isEmpty() )
        {
            return Collections.emptyMap();
        }

        Collector<Map.Entry<String,Object>,HashMap<String,Object>,HashMap<String,Object>> toHashMap = Collector.of( //
                HashMap::new,  //
                ( map, e ) -> map.put( e.getKey(), mapToValue( e.getValue() ) ),  //
                ( map1, map2 ) ->
                {  //
                    map1.putAll( map2 );  //
                    return map1;  //
                } );
        return originalParameters.entrySet().stream().collect( Collectors.collectingAndThen( toHashMap, Collections::unmodifiableMap ) );
    }

    private static Object mapToValue( Object o )
    {

        if ( o instanceof org.neo4j.driver.v1.Value )
        {
            return mapToValue( ((org.neo4j.driver.v1.Value) o).asObject() );
        }

        if ( o instanceof Collection )
        {
            return ((Collection) o).stream().map( DefaultEmbeddedCypherRunner::mapToValue ).toArray( Object[]::new );
        }

        if ( o instanceof InternalPoint2D )
        {
            InternalPoint2D point = (InternalPoint2D) o;
            return Values.pointValue( CoordinateReferenceSystem.get( point.srid() ), point.x(), point.y() );
        }

        if ( o instanceof InternalPoint3D )
        {
            InternalPoint3D point = (InternalPoint3D) o;
            return Values.pointValue( CoordinateReferenceSystem.get( point.srid() ), point.x(), point.y(), point.z() );
        }

        if ( o instanceof InternalIsoDuration )
        {
            InternalIsoDuration duration = (InternalIsoDuration) o;
            return Values.durationValue( duration );
        }

        return o;
    }
}
