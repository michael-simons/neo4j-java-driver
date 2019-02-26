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
package org.neo4j.driver.v1.integration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

public class EmbeddedNestedQueriesIT implements NestedQueries
{
    private static Driver embeddedDriver;
    private static Session session;

    @BeforeAll
    static void openEmbeddedDriver( @TempDir File temporaryBaseDirectory )
    {
        File graphDb = new File( temporaryBaseDirectory, "graph.db" );
        embeddedDriver = GraphDatabase.driver( graphDb.toURI(), AuthTokens.none(), Config.builder().withoutEncryption().toConfig() );
        session = embeddedDriver.session();
    }

    @Override
    public Session newSession( AccessMode mode )
    {
        return embeddedDriver.session(  );
    }

    @AfterAll
    static void closeEmbeddedDriver()
    {
        session.close();
        embeddedDriver.close();
    }
}
