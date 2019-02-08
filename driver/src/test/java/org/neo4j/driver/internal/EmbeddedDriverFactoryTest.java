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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.util.Map;

import org.neo4j.driver.v1.AuthTokens;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EmbeddedDriverFactoryTest
{
    @Nested
    class URIParsing
    {
        @Test
        void shouldExtractStoreDirFromURI()
        {
            File storeDir = EmbeddedDriverFactory.extractStoreDir( URI.create( "file:///foo/bar/graph.db" ) );
            assertEquals( "/foo/bar/graph.db", storeDir.getAbsolutePath() );
        }

        @Test
        void shouldExtractStoreDirFromURIWithAdditionalInformation()
        {
            File storeDir = EmbeddedDriverFactory.extractStoreDir( URI.create( "file:///foo/bar/graph.db?a=b&c=d" ) );
            assertEquals( "/foo/bar/graph.db", storeDir.getAbsolutePath() );
        }

        @Test
        void shouldRejectRelevativeFileUrls()
        {
            assertThrows( IllegalArgumentException.class, () ->  EmbeddedDriverFactory.extractStoreDir( URI.create( "file:///foo/../bar/graph.db" ) ));
        }

        @Test
        void shouldHandleNullQueryString()
        {
            Map<String,String> parameters = EmbeddedDriverFactory.extractQueryParameters( URI.create( "file:///foobar.txt" ) );
            assertTrue( parameters.isEmpty() );
        }

        @Test
        void shouldHandleEmptyQueryString()
        {

            Map<String,String> parameters = EmbeddedDriverFactory.extractQueryParameters( URI.create( "file:///foobar.txt?" ) );
            assertTrue( parameters.isEmpty() );
        }

        @Test
        void shouldHandleEmptyVarsString()
        {
            Map<String,String> parameters = EmbeddedDriverFactory.extractQueryParameters( URI.create( "file:///foobar.txt?a=" ) );
            assertFalse( parameters.containsKey( "a" ) );
        }

        @Test
        void shouldParseParameters()
        {
            Map<String,String> parameters = EmbeddedDriverFactory.extractQueryParameters( URI.create( "file:///foobar.txt?a=b&c=%2Fbazbar.conf&d=e" ) );
            assertEquals( "b", parameters.get( "a" ) );
            assertEquals( "/bazbar.conf", parameters.get( "c" ) );
            assertEquals( "e", parameters.get( "d" ) );
        }
    }

    @Nested
    class ParamterChecking
    {
        @Test
        void shouldRequireSupportedAuthToken()
        {
            assertDoesNotThrow( () -> EmbeddedDriverFactory.requireSupportedAuthToken( null ) );
            assertDoesNotThrow( () -> EmbeddedDriverFactory.requireSupportedAuthToken( AuthTokens.none() ) );
            assertThrows( IllegalArgumentException.class, () -> EmbeddedDriverFactory.requireSupportedAuthToken( AuthTokens.basic( "not", "supported" ) ) );
        }
    }
}