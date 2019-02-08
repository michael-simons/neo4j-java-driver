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

import org.neo4j.driver.internal.spi.DriverFactory;
import org.neo4j.driver.internal.spi.DriverFactoryProvider;

import static org.neo4j.driver.internal.EmbeddedDriverFactory.EMBEDDED_URI_SCHEMES;
import static org.neo4j.driver.internal.spi.DriverFactoryProvider.extractScheme;

/**
 * Provides access to the embedded driver's factory through the driver factory provider SPI.
 *
 * @since 2.0
 */
public class EmbeddedDriverFactoryProvider implements DriverFactoryProvider
{
    @Override
    public DriverFactory get()
    {
        return new EmbeddedDriverFactory();
    }

    @Override
    public boolean isSupportedScheme( URI uri )
    {
        return EMBEDDED_URI_SCHEMES.contains( extractScheme( uri ) );
    }
}
