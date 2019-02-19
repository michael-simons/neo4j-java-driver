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

import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Metrics;
import org.neo4j.driver.v1.Session;

import static org.neo4j.driver.internal.EmbeddedSession.BOOKMARKS_NOT_SUPPORTED_MESSAGE;

/**
 * A driver that is based on an embedded Neo4j instance in the same JVM.
 *
 * @since 2.0
 */
class EmbeddedDriver extends AbstractDriver implements Driver
{

    EmbeddedDriver(  EmbeddedSessionFactory sessionFactory, MetricsProvider metricsProvider, Logging logging )
    {
        super( sessionFactory, metricsProvider, logging );

    }

    @Override
    public boolean isEncrypted()
    {
        return false;
    }

    @Override
    public Session session()
    {
        return session( AccessMode.WRITE );
    }

    @Override
    public Session session( AccessMode mode )
    {
        return sessionFactory.newInstance( mode, Bookmarks.empty() );
    }

    @Override
    public Session session( String bookmark )
    {
        throw new UnsupportedOperationException( BOOKMARKS_NOT_SUPPORTED_MESSAGE );
    }

    @Override
    public Session session( AccessMode mode, String bookmark )
    {
        throw new UnsupportedOperationException( BOOKMARKS_NOT_SUPPORTED_MESSAGE );
    }

    @Override
    public Session session( Iterable<String> bookmarks )
    {
        throw new UnsupportedOperationException( BOOKMARKS_NOT_SUPPORTED_MESSAGE );
    }

    @Override
    public Session session( AccessMode mode, Iterable<String> bookmarks )
    {
        throw new UnsupportedOperationException( BOOKMARKS_NOT_SUPPORTED_MESSAGE );
    }

    @Override
    public Metrics metrics()
    {
        return null;
    }
}
