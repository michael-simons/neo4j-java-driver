package org.neo4j.driver.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.neo4j.driver.v1.exceptions.ClientException;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

@ExtendWith( MockitoExtension.class )
class EmbeddedTransactionTest
{
    @Mock
    EmbeddedCypherRunner cypherRunner;

    @Mock
    org.neo4j.graphdb.Transaction transaction;

    @Test
    void shouldRollbackOnImplicitFailure()
    {
        // Given
        EmbeddedTransaction tx = new EmbeddedTransaction( cypherRunner, () -> transaction );

        // When
        tx.close();

        // Then
        verify( transaction ).failure();
        verify( transaction ).close();
    }

    @Test
    void shouldRollbackOnExplicitFailure()
    {
        // Given
        EmbeddedTransaction tx = new EmbeddedTransaction( cypherRunner, () -> transaction );

        // When
        tx.failure();
        tx.success(); // even if success is called after the failure call!
        tx.close();

        // Then
        verify( transaction ).failure();
        verify( transaction ).close();
    }

    @Test
    void shouldCommitOnSuccess()
    {
        // Given
        EmbeddedTransaction tx = new EmbeddedTransaction( cypherRunner, () -> transaction );

        // When
        tx.success();
        tx.close();

        // Then
        verify( transaction ).success();
        verify( transaction ).close();
    }

    @Test
    void shouldBeOpenAfterConstruction()
    {
        EmbeddedTransaction tx = new EmbeddedTransaction( cypherRunner, () -> transaction );

        assertTrue( tx.isOpen() );
    }

    @Test
    void shouldBeOpenWhenMarkedForSuccess()
    {
        EmbeddedTransaction tx = new EmbeddedTransaction( cypherRunner, () -> transaction );

        tx.success();

        assertTrue( tx.isOpen() );
    }

    @Test
    void shouldBeOpenWhenMarkedForFailure()
    {
        EmbeddedTransaction tx = new EmbeddedTransaction( cypherRunner, () -> transaction );

        tx.failure();

        assertTrue( tx.isOpen() );
    }

    @Test
    void shouldBeClosedWhenMarkedAsTerminated()
    {
        EmbeddedTransaction tx = new EmbeddedTransaction( cypherRunner, () -> transaction );

        tx.markTerminated();

        assertTrue( tx.isOpen() );
    }

    @Test
    void shouldBeClosedAfterCommit()
    {
        EmbeddedTransaction tx = new EmbeddedTransaction( cypherRunner, () -> transaction );

        tx.success();
        tx.close();

        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldBeClosedAfterRollback()
    {
        EmbeddedTransaction tx = new EmbeddedTransaction( cypherRunner, () -> transaction );

        tx.failure();
        tx.close();

        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldBeClosedWhenMarkedTerminatedAndClosed()
    {
        EmbeddedTransaction tx = new EmbeddedTransaction( cypherRunner, () -> transaction );

        tx.markTerminated();
        tx.close();

        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldNotCommitWhenMarkedAsTerminated()
    {
        EmbeddedTransaction tx = new EmbeddedTransaction( cypherRunner, () -> transaction );

        tx.markTerminated();

        ClientException clientException = assertThrows( ClientException.class, () -> tx.close() );

        assertEquals( "Transaction can't be committed. It has been rolled back either because of an error or explicit termination",
                clientException.getMessage() );
    }
}