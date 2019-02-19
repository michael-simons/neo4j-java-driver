package org.neo4j.driver.internal;

import java.util.Map;
import java.util.stream.StreamSupport;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.stream.Collectors.toList;

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

    static EmbeddedCypherRunner createRunner( GraphDatabaseService graphDatabaseService )
    {
        return new DefaultEmbeddedCypherRunner( (GraphDatabaseFacade) graphDatabaseService );
    }
}

class DefaultEmbeddedCypherRunner implements EmbeddedCypherRunner
{

    private final GraphDatabaseFacade graphDatabaseFacade;

    public DefaultEmbeddedCypherRunner( GraphDatabaseFacade graphDatabaseFacade )
    {
        this.graphDatabaseFacade = graphDatabaseFacade;
    }

    @Override
    public Result execute( String query, Map<String,Object> parameters ) throws QueryExecutionException
    {
        return graphDatabaseFacade.execute( graphDatabaseFacade.beginTransaction( Transaction.Type.implicit, LoginContext.AUTH_DISABLED ), query,
                convertParameters( parameters ) );
    }

    private static MapValue convertParameters( Map<String,Object> originalParameters )
    {

        MapValueBuilder builder = new MapValueBuilder( originalParameters.size() );
        for ( Map.Entry<String,Object> entry : originalParameters.entrySet() )
        {
            builder.add( entry.getKey(), mapToValue( entry.getValue() ) );
        }

        return builder.build();
    }

    private static AnyValue mapToValue( Object o )
    {

        if ( o instanceof org.neo4j.driver.v1.Value )
        {
            return mapToValue( ((org.neo4j.driver.v1.Value) o).asObject() );
        }

        if ( o instanceof Iterable )
        {
            return VirtualValues.fromList(
                    StreamSupport.stream( (((Iterable<?>) o)).spliterator(), false ).map( DefaultEmbeddedCypherRunner::mapToValue ).collect( toList() ) );
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

        return ValueUtils.asAnyValue( o );
    }
}
