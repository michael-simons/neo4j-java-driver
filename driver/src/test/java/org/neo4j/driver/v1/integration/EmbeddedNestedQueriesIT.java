package org.neo4j.driver.v1.integration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
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
    private static final int LONG_VALUE_SIZE = 1_000_000;

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
