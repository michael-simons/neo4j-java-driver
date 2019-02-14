package org.neo4j.driver.internal;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;

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
        return graphDatabaseService.execute( query, parameters );
    }

    public Result execute( String query, Map<String,Object> parameters, long timeout, TimeUnit unit ) throws QueryExecutionException
    {
        return graphDatabaseService.execute( query, parameters, timeout, unit );
    }
}
