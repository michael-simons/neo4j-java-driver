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
package org.neo4j.driver.v1;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.junit.MatcherAssert.assertThat;

/**
 * This is a WIP class. If the embedded connector stays inside drivers, the content of this class should rather be moved into {@link GraphDatabaseTest} and the
 * matchers defined here go into {@link org.neo4j.driver.internal.util.Matchers}.
 */
class GraphDatabaseEmbeddedTest
{
    @TempDir
    File temporaryBaseDirectory;

    @Test
    void fileSchemeShouldInstantiateDirectDriver()
    {
        File graphDb = new File( temporaryBaseDirectory, "graph.db" );

        try ( Driver driver = GraphDatabase.driver( graphDb.toURI(), Config.builder().withoutEncryption().toConfig() ) )
        {
            assertThat( driver, is( embeddedDriver() ) );
        }
    }

    public static Matcher<Driver> embeddedDriver()
    {
        return new TypeSafeMatcher<Driver>()
        {
            @Override
            protected boolean matchesSafely( Driver driver )
            {
                return driver.getClass().getName().equals( "org.neo4j.driver.internal.EmbeddedDriver" );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "embedded 'file://' driver " );
            }
        };
    }
}
