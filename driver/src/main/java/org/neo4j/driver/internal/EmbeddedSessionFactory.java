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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * @since 2.0
 */
public class EmbeddedSessionFactory implements SessionFactory
{
    private final EmbeddedSessionSettings settings;
    private final GraphDatabaseService graphDatabaseService;
    private final RetryLogic retryLogic;
    private final Logging logging;

    public EmbeddedSessionFactory( EmbeddedSessionSettings settings, RetryLogic retryLogic, GraphDatabaseService graphDatabaseService, Logging logging )
    {
        this.settings = settings;
        this.graphDatabaseService = graphDatabaseService;
        this.retryLogic = retryLogic;
        this.logging = logging;
    }

    @Override
    public Session newInstance( AccessMode mode, Bookmarks bookmarks )
    {
        requireSupportedAccessMode( mode );
        return new EmbeddedSession( graphDatabaseService, retryLogic, logging );
    }

    @Override
    public CompletionStage<Void> verifyConnectivity()
    {
        return CompletableFuture.runAsync( () -> this.graphDatabaseService.isAvailable( settings.connectionAcquisitionTimeout().toMillis() ) );
    }

    @Override
    public CompletionStage<Void> close()
    {
        return CompletableFuture.runAsync( () -> graphDatabaseService.shutdown() );
    }

    static void requireSupportedAccessMode( AccessMode accessMode )
    {
        if ( accessMode != AccessMode.WRITE )
        {
            throw new IllegalArgumentException( "Embedded driver does not support READ access mode." );
        }
    }
}
