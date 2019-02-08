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

import io.netty.bootstrap.Bootstrap;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.neo4j.driver.internal.async.BootstrapFactory;
import org.neo4j.driver.internal.cluster.RoutingSettings;
import org.neo4j.driver.internal.logging.NettyLogging;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.retry.ExponentialBackoffRetryLogic;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.retry.RetrySettings;
import org.neo4j.driver.internal.spi.DriverFactory;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

/**
 * @since 2.0
 */
class EmbeddedDriverFactory implements DriverFactory
{
    static final List<String> EMBEDDED_URI_SCHEMES = Collections.singletonList( "file" );

    @Override
    public Driver newInstance( URI uri, AuthToken authToken, RoutingSettings routingSettings, RetrySettings retrySettings, Config config )
    {
        Objects.requireNonNull( uri, "URI can't be null" );
        requireSupportedAuthToken( authToken );
        requireInsecureSecurityPlan( config );

        File storeDir = extractStoreDir( uri );
        Map<String,String> parameters = extractQueryParameters( uri );
        GraphDatabaseBuilder graphDatabaseBuilder = createGraphDatabaseBuilder( storeDir, parameters );
        graphDatabaseBuilder = configureGraphDatabaseBuilder( graphDatabaseBuilder, parameters );
        RetryLogic retryLogic = createRetryLogic( retrySettings, config );

        EmbeddedSessionFactory sessionFactory = createSessionFacory( retryLogic, graphDatabaseBuilder, config );
        MetricsProvider metricsProvider = MetricsProvider.METRICS_DISABLED_PROVIDER;

        return new EmbeddedDriver( sessionFactory, metricsProvider, config.logging() );
    }

    static RetryLogic createRetryLogic( RetrySettings retrySettings, Config config )
    {
        InternalLoggerFactory.setDefaultFactory( new NettyLogging( config.logging() ) );
        Bootstrap bootstrap = BootstrapFactory.newBootstrap();
        EventExecutorGroup eventExecutorGroup = bootstrap.config().group();
        return new ExponentialBackoffRetryLogic( retrySettings, eventExecutorGroup, Clock.SYSTEM, config.logging() );
    }

    static GraphDatabaseBuilder createGraphDatabaseBuilder( File storeDir, Map<String,String> parameters )
    {
        // TODO Place to chose edition
        return new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir );
    }

    static GraphDatabaseBuilder configureGraphDatabaseBuilder( GraphDatabaseBuilder builder, Map<String,String> parameters )
    {
        return builder;
    }

    static EmbeddedSessionFactory createSessionFacory( RetryLogic retryLogic, GraphDatabaseBuilder builder, Config config )
    {
        EmbeddedSessionSettings settings = new EmbeddedSessionSettings( config );
        return new EmbeddedSessionFactory( settings, retryLogic, builder.newGraphDatabase(), config.logging() );
    }

    static Optional<File> extractNeo4jConfLocation( URI uri )
    {
        return Optional.empty();
    }

    static Map<String,String> extractQueryParameters( URI uri )
    {
        Optional<String> rawQuery = Optional.ofNullable( uri.getQuery() ).map( String::trim ).filter( rq -> !rq.isEmpty() );
        String[] parts = rawQuery.map( rq -> rq.split( "&" ) ).orElse( new String[0] );
        return Arrays.stream( parts ).map( p -> p.split( "=" ) ).filter( p -> p.length == 2 ).collect(
                collectingAndThen( toMap( p -> p[0], p -> p[1] ), Collections::unmodifiableMap ) );
    }

    static File extractStoreDir( URI uri )
    {
        requireAbsoluteUrls( uri );

        URI fileUri = URI.create( String.format( "%s://%s", uri.getScheme(), uri.getPath() ) );
        return new File( fileUri );
    }

    static void requireSupportedAuthToken( AuthToken tokenToAssert )
    {
        if ( !(tokenToAssert == null || tokenToAssert.equals( AuthTokens.none() )) )
        {
            throw new IllegalArgumentException( "Embedded connector doesn't support authentication." );
        }
    }

    static void requireAbsoluteUrls( URI uri )
    {
        URI normalized = uri.normalize();
        if ( !normalized.equals( uri ) )
        {
            throw new IllegalArgumentException( "URI must be absolute." );
        }
    }

    static void requireInsecureSecurityPlan( Config config )
    {
        if ( config.encrypted() )
        {
            throw new IllegalArgumentException( "Embedded driver starts an embedded database and cannot encrypt in-memory structures." );
        }
    }
}
