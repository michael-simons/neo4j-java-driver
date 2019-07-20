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

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.internal.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.spi.ConnectionPool;
import org.neo4j.driver.internal.util.FakeClock;
import org.neo4j.driver.internal.util.Futures;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.AccessMode.READ;
import static org.neo4j.driver.AccessMode.WRITE;
import static org.neo4j.driver.internal.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.logging.DevNullLogger.DEV_NULL_LOGGER;
import static org.neo4j.driver.internal.messaging.request.MultiDatabaseUtil.ABSENT_DB_NAME;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.A;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.B;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.C;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.D;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.E;
import static org.neo4j.driver.internal.util.ClusterCompositionUtil.F;
import static org.neo4j.driver.util.TestUtil.asOrderedSet;
import static org.neo4j.driver.util.TestUtil.await;

class RoutingTableHandlerTest
{
    @Test
    void shouldRemoveAddressFromRoutingTableOnConnectionFailure()
    {
        RoutingTable routingTable = new ClusterRoutingTable( ABSENT_DB_NAME, new FakeClock() );
        routingTable.update( new ClusterComposition(
                42, asOrderedSet( A, B, C ), asOrderedSet( A, C, E ), asOrderedSet( B, D, F ) ) );

        RoutingTableHandler handler =
                new RoutingTableHandler( routingTable, newRediscoveryMock(), newConnectionPoolMock(), newRoutingTablesMock(), DEV_NULL_LOGGER );


        handler.onConnectionFailure( B );

        assertArrayEquals( new BoltServerAddress[]{A, C}, routingTable.readers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{A, C, E}, routingTable.writers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{D, F}, routingTable.routers().toArray() );

        handler.onConnectionFailure( A );

        assertArrayEquals( new BoltServerAddress[]{C}, routingTable.readers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{C, E}, routingTable.writers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{D, F}, routingTable.routers().toArray() );
    }

    @Test
    void acquireShouldUpdateRoutingTableWhenKnownRoutingTableIsStale()
    {
        BoltServerAddress initialRouter = new BoltServerAddress( "initialRouter", 1 );
        BoltServerAddress reader1 = new BoltServerAddress( "reader-1", 2 );
        BoltServerAddress reader2 = new BoltServerAddress( "reader-1", 3 );
        BoltServerAddress writer1 = new BoltServerAddress( "writer-1", 4 );
        BoltServerAddress router1 = new BoltServerAddress( "router-1", 5 );

        ConnectionPool connectionPool = newConnectionPoolMock();
        ClusterRoutingTable routingTable = new ClusterRoutingTable( ABSENT_DB_NAME, new FakeClock(), initialRouter );

        Set<BoltServerAddress> readers = new LinkedHashSet<>( asList( reader1, reader2 ) );
        Set<BoltServerAddress> writers = new LinkedHashSet<>( singletonList( writer1 ) );
        Set<BoltServerAddress> routers = new LinkedHashSet<>( singletonList( router1 ) );
        ClusterComposition clusterComposition = new ClusterComposition( 42, readers, writers, routers );
        Rediscovery rediscovery = mock( RediscoveryImpl.class );
        when( rediscovery.lookupClusterComposition( routingTable, connectionPool ) )
                .thenReturn( completedFuture( clusterComposition ) );

        RoutingTableHandler handler = new RoutingTableHandler( routingTable, rediscovery, connectionPool,
                newRoutingTablesMock(), DEV_NULL_LOGGER );

        assertNotNull( await( handler.refreshRoutingTable( READ ) ) );

        verify( rediscovery ).lookupClusterComposition( routingTable, connectionPool );
        assertArrayEquals( new BoltServerAddress[]{reader1, reader2}, routingTable.readers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{writer1}, routingTable.writers().toArray() );
        assertArrayEquals( new BoltServerAddress[]{router1}, routingTable.routers().toArray() );
    }

    @Test
    void shouldRediscoverOnReadWhenRoutingTableIsStaleForReads()
    {
        testRediscoveryWhenStale( READ );
    }

    @Test
    void shouldRediscoverOnWriteWhenRoutingTableIsStaleForWrites()
    {
        testRediscoveryWhenStale( WRITE );
    }

    @Test
    void shouldNotRediscoverOnReadWhenRoutingTableIsStaleForWritesButNotReads()
    {
        testNoRediscoveryWhenNotStale( WRITE, READ );
    }

    @Test
    void shouldNotRediscoverOnWriteWhenRoutingTableIsStaleForReadsButNotWrites()
    {
        testNoRediscoveryWhenNotStale( READ, WRITE );
    }

    @Test
    void shouldRetainAllFetchedAddressesInConnectionPoolAfterFetchingOfRoutingTable()
    {
        RoutingTable routingTable = new ClusterRoutingTable( ABSENT_DB_NAME, new FakeClock() );
        routingTable.update( new ClusterComposition(
                42, asOrderedSet(), asOrderedSet( B, C ), asOrderedSet( D, E ) ) );

        ConnectionPool connectionPool = newConnectionPoolMock();

        Rediscovery rediscovery = newRediscoveryMock();
        when( rediscovery.lookupClusterComposition( any(), any() ) ).thenReturn( completedFuture(
                new ClusterComposition( 42, asOrderedSet( A, B ), asOrderedSet( B, C ), asOrderedSet( A, C ) ) ) );

        RoutingTableRegistry routingTables = new RoutingTableRegistry()
        {
            @Override
            public CompletionStage<RoutingTableHandler> refreshRoutingTable( String databaseName, AccessMode mode )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Set<BoltServerAddress> allServers()
            {
                return routingTable.servers();
            }

            @Override
            public void remove( String databaseName )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void purgeAged()
            {
            }
        };

        RoutingTableHandler handler = new RoutingTableHandler( routingTable, rediscovery, connectionPool,
                routingTables, DEV_NULL_LOGGER );

        RoutingTable actual = await( handler.refreshRoutingTable( READ ) );
        assertEquals( routingTable, actual );

        verify( connectionPool ).retainAll( new HashSet<>( asList( A, B, C ) ) );
    }

    @Test
    void shouldRemoveRoutingTableHandlerIfFailedToLookup() throws Throwable
    {
        // Given
        RoutingTable routingTable = new ClusterRoutingTable( ABSENT_DB_NAME, new FakeClock() );

        Rediscovery rediscovery = newRediscoveryMock();
        when( rediscovery.lookupClusterComposition( any(), any() ) ).thenReturn( Futures.failedFuture( new RuntimeException( "Bang!" ) ) );

        ConnectionPool connectionPool = newConnectionPoolMock();
        RoutingTableRegistry routingTables = newRoutingTablesMock();
        // When

        RoutingTableHandler handler = new RoutingTableHandler( routingTable, rediscovery, connectionPool, routingTables, DEV_NULL_LOGGER );
        assertThrows( RuntimeException.class, () -> await( handler.refreshRoutingTable( READ ) ) );

        // Then
        verify( routingTables ).remove( ABSENT_DB_NAME );
    }

    private void testRediscoveryWhenStale( AccessMode mode )
    {
        ConnectionPool connectionPool = mock( ConnectionPool.class );
        when( connectionPool.acquire( LOCAL_DEFAULT ) )
                .thenReturn( completedFuture( mock( Connection.class ) ) );

        RoutingTable routingTable = newStaleRoutingTableMock( mode );
        Rediscovery rediscovery = newRediscoveryMock();

        RoutingTableHandler handler = new RoutingTableHandler( routingTable, rediscovery, connectionPool,
                newRoutingTablesMock(), DEV_NULL_LOGGER );
        RoutingTable actual = await( handler.refreshRoutingTable( mode ) );
        assertEquals( routingTable, actual );

        verify( routingTable ).isStaleFor( mode );
        verify( rediscovery ).lookupClusterComposition( routingTable, connectionPool );
    }

    private void testNoRediscoveryWhenNotStale( AccessMode staleMode, AccessMode notStaleMode )
    {
        ConnectionPool connectionPool = mock( ConnectionPool.class );
        when( connectionPool.acquire( LOCAL_DEFAULT ) )
                .thenReturn( completedFuture( mock( Connection.class ) ) );

        RoutingTable routingTable = newStaleRoutingTableMock( staleMode );
        Rediscovery rediscovery = newRediscoveryMock();

        RoutingTableHandler handler = new RoutingTableHandler( routingTable, rediscovery, connectionPool,
                newRoutingTablesMock(), DEV_NULL_LOGGER );

        assertNotNull( await( handler.refreshRoutingTable( notStaleMode ) ) );
        verify( routingTable ).isStaleFor( notStaleMode );
        verify( rediscovery, never() ).lookupClusterComposition( routingTable, connectionPool );
    }

    private static RoutingTable newStaleRoutingTableMock( AccessMode mode )
    {
        RoutingTable routingTable = mock( RoutingTable.class );
        when( routingTable.isStaleFor( mode ) ).thenReturn( true );

        AddressSet addresses = new AddressSet();
        addresses.update( new HashSet<>( singletonList( LOCAL_DEFAULT ) ) );
        when( routingTable.readers() ).thenReturn( addresses );
        when( routingTable.writers() ).thenReturn( addresses );

        return routingTable;
    }

    private static RoutingTableRegistry newRoutingTablesMock()
    {
        return mock( RoutingTableRegistry.class );
    }

    private static Rediscovery newRediscoveryMock()
    {
        Rediscovery rediscovery = mock( RediscoveryImpl.class );
        Set<BoltServerAddress> noServers = Collections.emptySet();
        ClusterComposition clusterComposition = new ClusterComposition( 1, noServers, noServers, noServers );
        when( rediscovery.lookupClusterComposition( any( RoutingTable.class ), any( ConnectionPool.class ) ) )
                .thenReturn( completedFuture( clusterComposition ) );
        return rediscovery;
    }

    private static ConnectionPool newConnectionPoolMock()
    {
        return newConnectionPoolMockWithFailures( emptySet() );
    }

    private static ConnectionPool newConnectionPoolMockWithFailures(
            Set<BoltServerAddress> unavailableAddresses )
    {
        ConnectionPool pool = mock( ConnectionPool.class );
        when( pool.acquire( any( BoltServerAddress.class ) ) ).then( invocation ->
        {
            BoltServerAddress requestedAddress = invocation.getArgument( 0 );
            if ( unavailableAddresses.contains( requestedAddress ) )
            {
                return Futures.failedFuture( new ServiceUnavailableException( requestedAddress + " is unavailable!" ) );
            }
            Connection connection = mock( Connection.class );
            when( connection.serverAddress() ).thenReturn( requestedAddress );
            return completedFuture( connection );
        } );
        return pool;
    }
}
