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

import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Metrics;
import org.neo4j.driver.v1.Session;

import static org.neo4j.driver.internal.util.Futures.completedWithNull;

/**
 * Shared supported for creating {@link Driver driver-implementations}.
 *
 * Drivers extending this support class are created as "not closed", that is open and ready to serve session.
 *
 * @since 2.0
 */
abstract class AbstractDriver implements Driver
{
    protected final SessionFactory sessionFactory;
    protected final MetricsProvider metricsProvider;
    protected final Logger log;

    protected final AtomicBoolean closed = new AtomicBoolean( false );

    AbstractDriver( SessionFactory sessionFactory, MetricsProvider metricsProvider, Logging logging )
    {
        Objects.requireNonNull( sessionFactory, "Session factory can't be null" );
        Objects.requireNonNull( metricsProvider, "Metrics provider can't be null" );
        Objects.requireNonNull( logging, "Logging can't be null" );

        this.sessionFactory = sessionFactory;
        this.metricsProvider = metricsProvider;
        this.log = logging.getLog( this.getClass().getSimpleName() );
    }

    @Override
    public Session session()
    {
        return session( AccessMode.WRITE );
    }

    @Override
    public Session session( AccessMode mode )
    {
        return newSession( mode, Bookmarks.empty() );
    }

    @Override
    public Session session( String bookmark )
    {
        return session( AccessMode.WRITE, bookmark );
    }

    @Override
    public Session session( AccessMode mode, String bookmark )
    {
        return newSession( mode, Bookmarks.from( bookmark ) );
    }

    @Override
    public Session session( Iterable<String> bookmarks )
    {
        return session( AccessMode.WRITE, bookmarks );
    }

    @Override
    public Session session( AccessMode mode, Iterable<String> bookmarks )
    {
        return newSession( mode, Bookmarks.from( bookmarks ) );
    }

    @Override
    public Metrics metrics()
    {
        return metricsProvider.metrics();
    }

    @Override
    public void close()
    {
        Futures.blockingGet( closeAsync() );
    }

    @Override
    public CompletionStage<Void> closeAsync()
    {
        if ( closed.compareAndSet( false, true ) )
        {
            log.info( "Closing driver instance %s", hashCode() );
            return sessionFactory.close();
        }
        return completedWithNull();
    }

    private Session newSession( AccessMode mode, Bookmarks bookmarks )
    {
        assertOpen();
        Session session = sessionFactory.newInstance( mode, bookmarks );
        // Make sure the driver hasn't been close in between
        assertOpen();
        return session;
    }

    void assertOpen()
    {
        if ( closed.get() )
        {
            throw driverCloseException();
        }
    }

    static RuntimeException driverCloseException()
    {
        return new IllegalStateException( "This driver instance has already been closed" );
    }
}
