/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.util.TestNeo4j;

public class NestedQueriesIT extends NestedQueries
{
    private final TestNeo4j server = new TestNeo4j();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( server ).around( Timeout.seconds( 120 ) );

    @Override
    public Session newSession( AccessMode mode )
    {
        return server.driver().session( mode );
    }
}