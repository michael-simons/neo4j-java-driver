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
package org.neo4j.driver.internal.spi;

import java.net.URI;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Supplier;

public interface DriverFactoryProvider extends Supplier<DriverFactory>
{
    /**
     * Get the order value of this Driver Factory.
     * Higher values are interpreted as lower priority. As a consequence, the factory implementation with
     * the lowest value has the highest priority. Same order values will result in arbitrary sort positions
     * for the affected objects.
     *
     * @return the order value
     */
    default int getOrder()
    {
        return Integer.MAX_VALUE;
    }

    default boolean isSupportedScheme( URI uri )
    {
        return false;
    }

    /**
     * Static helper method for providers as well as for factories itself to extract the scheme fron an URI.
     *
     * @param uri
     * @return The scheme of the URI in {@link Locale#ENGLISH english} lower case.
     */
    static String extractScheme( URI uri )
    {
        Objects.requireNonNull( uri, "Uri cannot be null." );
        return uri.getScheme().toLowerCase( Locale.ENGLISH );
    }
}
