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
package org.neo4j.driver.internal.cluster;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.util.Clock;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RoutingTableFactoryImplTest
{
    @Test
    void shouldCreateEmptyRoutingTableForDatabase() throws Throwable
    {
        // Given
        Clock clock = Clock.SYSTEM;
        BoltServerAddress initialRouter = new BoltServerAddress( "localhost:7687" );
        RoutingTableFactoryImpl factory = new RoutingTableFactoryImpl( initialRouter, clock );

        // When
        RoutingTable routingTable = factory.newInstance( "mydb" );

        // Then
        assertThat( routingTable.database(), equalTo( "mydb" ) );
        assertThat( routingTable.servers(), contains( equalTo( initialRouter ) ) );
        assertThat( Arrays.asList( routingTable.routers().toArray() ), contains( equalTo( initialRouter ) ) );

        assertThat( routingTable.readers().size(), equalTo( 0 ) );
        assertThat( routingTable.writers().size(), equalTo( 0 ) );

        assertTrue( routingTable.isStaleFor( AccessMode.READ ) );
        assertTrue( routingTable.isStaleFor( AccessMode.WRITE ) );
    }
}
