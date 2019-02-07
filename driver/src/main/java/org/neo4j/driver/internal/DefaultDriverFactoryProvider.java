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

import java.net.URI;

import org.neo4j.driver.internal.spi.DriverFactoryProvider;

import static org.neo4j.driver.internal.DefaultDriverFactory.BOLT_ROUTING_URI_SCHEME;
import static org.neo4j.driver.internal.DefaultDriverFactory.BOLT_URI_SCHEME;
import static org.neo4j.driver.internal.spi.DriverFactoryProvider.extractScheme;

/**
 * Provides the default drivers factory that support {@code bolt} and {@code bolt+routing} schemes.
 */
public final class DefaultDriverFactoryProvider implements DriverFactoryProvider
{
    @Override
    public boolean isSupportedScheme( URI uri )
    {
        String scheme = extractScheme( uri );
        return BOLT_URI_SCHEME.equals( scheme ) || BOLT_ROUTING_URI_SCHEME.equals( scheme );
    }

    @Override
    public DefaultDriverFactory get()
    {
        return new DefaultDriverFactory();
    }
}
