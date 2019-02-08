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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import java.util.logging.Level;

import org.neo4j.driver.internal.logging.ConsoleLogging;
import org.neo4j.driver.internal.retry.FixedRetryLogic;
import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.util.TxWork;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.graphdb.GraphDatabaseService;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.NetworkSessionTest.executeTransaction;
import static org.neo4j.driver.internal.NetworkSessionTest.verifyInvocationCount;
import static org.neo4j.driver.v1.AccessMode.READ;
import static org.neo4j.driver.v1.AccessMode.WRITE;

@ExtendWith( MockitoExtension.class )
class EmbeddedSessionTest
{
    private static final ConsoleLogging CONSOLE_LOGGING = new ConsoleLogging( Level.ALL );
    private static final FixedRetryLogic FIXED_RETRY_LOGIC = new FixedRetryLogic( 0 );

    @Mock( answer = Answers.RETURNS_MOCKS )
    GraphDatabaseService graphDatabaseService;

    @Mock
    org.neo4j.graphdb.Transaction transaction;

    @Test
    void shouldNotAllowNewTxWhileOneIsRunning()
    {

        EmbeddedSession session = new EmbeddedSession( graphDatabaseService, FIXED_RETRY_LOGIC, CONSOLE_LOGGING );
        session.beginTransaction();

        assertThrows( ClientException.class, session::beginTransaction );
    }

    @Test
    void shouldBeAbleToOpenTxAfterPreviousIsClosed()
    {
        // Given
        EmbeddedSession session = new EmbeddedSession( graphDatabaseService, FIXED_RETRY_LOGIC, CONSOLE_LOGGING );
        session.beginTransaction().close();

        // When
        Transaction tx = session.beginTransaction();

        // Then we should've gotten a transaction object back
        assertNotNull( tx );
    }

    @Test
    void shouldNotBeAbleToUseSessionWhileOngoingTransaction()
    {
        // Given
        EmbeddedSession session = new EmbeddedSession( graphDatabaseService, FIXED_RETRY_LOGIC, CONSOLE_LOGGING );
        session.beginTransaction();

        // Expect
        assertThrows( ClientException.class, () -> session.run( "RETURN 1" ) );
    }

    @Test
    void shouldNotCloseAlreadyClosedSession()
    {
        when( graphDatabaseService.beginTx() ).thenReturn( transaction );
        EmbeddedSession session = new EmbeddedSession( graphDatabaseService, FIXED_RETRY_LOGIC, CONSOLE_LOGGING );
        session.beginTransaction();

        session.close();
        session.close();
        session.close();

        verify( transaction, times( 1 ) ).close();
    }

    @Test
    void runThrowsWhenSessionIsClosed()
    {
        EmbeddedSession session = new EmbeddedSession( graphDatabaseService, FIXED_RETRY_LOGIC, CONSOLE_LOGGING );

        session.close();

        ClientException e = assertThrows( ClientException.class, () -> session.run( "CREATE ()" ) );
        assertThat( e.getMessage(), containsString( "session is already closed" ) );
    }

    @SuppressWarnings( "deprecation" )
    @Test
    void shouldThrowUnsupportedOperationWhenUsingBookmarks()
    {
        EmbeddedSession session = new EmbeddedSession( graphDatabaseService, FIXED_RETRY_LOGIC, CONSOLE_LOGGING );

        assertThrows( UnsupportedOperationException.class, () -> session.lastBookmark() );
        assertThrows( UnsupportedOperationException.class, () -> session.beginTransaction( "my-bookmark" ) );
    }

    @Test
    void commitsReadTxWhenMarkedSuccessful()
    {
        testTxCommitOrRollback( READ, true );
    }

    @Test
    void commitsWriteTxWhenMarkedSuccessful()
    {
        testTxCommitOrRollback( WRITE, true );
    }

    @Test
    void rollsBackReadTxWhenMarkedSuccessful()
    {
        testTxCommitOrRollback( READ, false );
    }

    @Test
    void rollsBackWriteTxWhenMarkedSuccessful()
    {
        testTxCommitOrRollback( READ, true );
    }

    @Test
    void rollsBackReadTxWhenFunctionThrows()
    {
        testTxRollbackWhenThrows( READ );
    }

    @Test
    void rollsBackWriteTxWhenFunctionThrows()
    {
        testTxRollbackWhenThrows( WRITE );
    }

    @Test
    void readTxRetriedUntilSuccessWhenFunctionThrows()
    {
        testTxIsRetriedUntilSuccessWhenFunctionThrows( READ );
    }

    @Test
    void writeTxRetriedUntilSuccessWhenFunctionThrows()
    {
        testTxIsRetriedUntilSuccessWhenFunctionThrows( WRITE );
    }

    @Test
    void readTxRetriedUntilSuccessWhenTxCloseThrows()
    {
        testTxIsRetriedUntilSuccessWhenCommitThrows( READ );
    }

    @Test
    void writeTxRetriedUntilSuccessWhenTxCloseThrows()
    {
        testTxIsRetriedUntilSuccessWhenCommitThrows( WRITE );
    }

    @Test
    void readTxRetriedUntilFailureWhenFunctionThrows()
    {
        testTxIsRetriedUntilFailureWhenFunctionThrows( READ );
    }

    @Test
    void writeTxRetriedUntilFailureWhenFunctionThrows()
    {
        testTxIsRetriedUntilFailureWhenFunctionThrows( WRITE );
    }

    @Test
    void readTxRetriedUntilFailureWhenTxCloseThrows()
    {
        testTxIsRetriedUntilFailureWhenCommitFails( READ );
    }

    @Test
    void writeTxRetriedUntilFailureWhenTxCloseThrows()
    {
        testTxIsRetriedUntilFailureWhenCommitFails( WRITE );
    }

    @Test
    void shouldNotAllowStartingMultipleTransactions()
    {
        EmbeddedSession session = new EmbeddedSession( graphDatabaseService, FIXED_RETRY_LOGIC, CONSOLE_LOGGING );

        Transaction tx = session.beginTransaction();
        assertNotNull( tx );

        for ( int i = 0; i < 5; i++ )
        {
            ClientException e = assertThrows( ClientException.class, session::beginTransaction );
            assertThat( e.getMessage(),
                    containsString( "You cannot begin a transaction on a session with an open transaction" ) );
        }
    }

    @Test
    void shouldAllowStartingTransactionAfterCurrentOneIsClosed()
    {
        EmbeddedSession session = new EmbeddedSession( graphDatabaseService, FIXED_RETRY_LOGIC, CONSOLE_LOGGING );

        Transaction tx = session.beginTransaction();
        assertNotNull( tx );

        ClientException e = assertThrows( ClientException.class, session::beginTransaction );
        assertThat( e.getMessage(),
                containsString( "You cannot begin a transaction on a session with an open transaction" ) );

        tx.close();

        assertNotNull( session.beginTransaction() );
    }

    private void testTxCommitOrRollback( AccessMode transactionMode, final boolean commit )
    {
        when( graphDatabaseService.beginTx() ).thenReturn( transaction );
        EmbeddedSession session = new EmbeddedSession( graphDatabaseService, FIXED_RETRY_LOGIC, CONSOLE_LOGGING );

        TransactionWork<Integer> work = tx ->
        {
            if ( commit )
            {
                tx.success();
            }
            else
            {
                tx.failure();
            }
            return 4242;
        };

        int result = executeTransaction( session, transactionMode, work );
        verify( graphDatabaseService, times( 1 ) ).beginTx();
        if ( commit )
        {
            verify( transaction, times( 1 ) ).success();
            verify( transaction, never() ).failure();
        }
        else
        {
            verify( transaction, times( 1 ) ).failure();
            verify( transaction, never() ).success();
        }
        assertEquals( 4242, result );
    }

    private void testTxRollbackWhenThrows( AccessMode transactionMode )
    {
        when( graphDatabaseService.beginTx() ).thenReturn( transaction );
        EmbeddedSession session = new EmbeddedSession( graphDatabaseService, FIXED_RETRY_LOGIC, CONSOLE_LOGGING );

        final RuntimeException exception = new RuntimeException( "¯\\_(ツ)_/¯" );
        TransactionWork<Void> work = tx ->
        {
            throw exception;
        };

        Exception exceptionThrown = assertThrows( Exception.class, () -> executeTransaction( session, transactionMode, work ) );
        assertEquals( exception, exceptionThrown );

        verify( graphDatabaseService, times( 1 ) ).beginTx();
        verify( transaction, times( 1 ) ).failure();
    }

    private void testTxIsRetriedUntilFailureWhenFunctionThrows( AccessMode mode )
    {
        int failures = 14;
        int retries = failures - 1;

        when( graphDatabaseService.beginTx() ).thenReturn( transaction );

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        EmbeddedSession session = new EmbeddedSession( graphDatabaseService, retryLogic, CONSOLE_LOGGING );

        TxWork work = spy( new TxWork( 42, failures, new SessionExpiredException( "Oh!" ) ) );

        Exception e = assertThrows( Exception.class, () -> executeTransaction( session, mode, work ) );

        assertThat( e, instanceOf( SessionExpiredException.class ) );
        assertEquals( "Oh!", e.getMessage() );
        verifyInvocationCount( work, failures );
        verify( transaction, never() ).success();
        verify( transaction, times( failures ) ).failure();
    }

    private void testTxIsRetriedUntilSuccessWhenCommitThrows( AccessMode mode )
    {
        int failures = 13;
        int retries = failures + 1;

        when( graphDatabaseService.beginTx() ).thenReturn( transaction );
        setupFailingCommit( transaction, failures );

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        EmbeddedSession session = new EmbeddedSession( graphDatabaseService, retryLogic, CONSOLE_LOGGING );

        TxWork work = spy( new TxWork( 43 ) );
        int answer = executeTransaction( session, mode, work );

        assertEquals( 43, answer );
        verifyInvocationCount( work, failures + 1 );
        verify( transaction, times( retries ) ).success();
    }

    private void testTxIsRetriedUntilSuccessWhenFunctionThrows( AccessMode mode )
    {
        int failures = 12;
        int retries = failures + 1;

        when( graphDatabaseService.beginTx() ).thenReturn( transaction );

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        EmbeddedSession session = new EmbeddedSession( graphDatabaseService, retryLogic, CONSOLE_LOGGING );

        TxWork work = spy( new TxWork( 42, failures, new SessionExpiredException( "" ) ) );
        int answer = executeTransaction( session, mode, work );

        assertEquals( 42, answer );
        verifyInvocationCount( work, failures + 1 );
        verify( transaction, times( 1 ) ).success();
        verify( transaction, times( failures ) ).failure();
    }

    private void testTxIsRetriedUntilFailureWhenCommitFails( AccessMode mode )
    {
        int failures = 17;
        int retries = failures - 1;

        when( graphDatabaseService.beginTx() ).thenReturn( transaction );
        setupFailingCommit( transaction, failures );

        RetryLogic retryLogic = new FixedRetryLogic( retries );
        EmbeddedSession session = new EmbeddedSession( graphDatabaseService, retryLogic, CONSOLE_LOGGING );

        TxWork work = spy( new TxWork( 42 ) );

        Exception e = assertThrows( Exception.class, () -> executeTransaction( session, mode, work ) );

        assertThat( e, instanceOf( ServiceUnavailableException.class ) );
        verifyInvocationCount( work, failures );
        verify( transaction, times( failures ) ).success();
    }

    private static void setupFailingCommit( org.neo4j.graphdb.Transaction transaction, int times )
    {
        doAnswer( new Answer<Void>()
        {
            int invoked;

            @Override
            public Void answer( InvocationOnMock invocation )
            {
                if ( invoked++ < times )
                {
                    throw new ServiceUnavailableException( "" );
                }
                return null;
            }
        } ).when( transaction ).close();
    }
}