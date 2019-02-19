package org.neo4j.driver.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.TimeUnit;

import org.neo4j.driver.v1.TransactionConfig;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith( MockitoExtension.class )
class EmbeddedTransactionTest
{
    @Mock
    GraphDatabaseFacade graphDatabaseService;

    @Mock
    org.neo4j.graphdb.Transaction transaction;

    @BeforeEach
    void prepareMocks()
    {
        when( graphDatabaseService.beginTx( ArgumentMatchers.anyLong(), ArgumentMatchers.any( TimeUnit.class ) ) ).thenReturn( transaction );
    }

    @Test
    void shouldRollbackOnImplicitFailure()
    {
        // Given
        EmbeddedTransaction tx = createTransaction();

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
        EmbeddedTransaction tx = createTransaction();

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
        EmbeddedTransaction tx = createTransaction();

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
        EmbeddedTransaction tx = createTransaction();

        assertTrue( tx.isOpen() );
    }

    @Test
    void shouldBeOpenWhenMarkedForSuccess()
    {
        EmbeddedTransaction tx = createTransaction();

        tx.success();

        assertTrue( tx.isOpen() );
    }

    @Test
    void shouldBeOpenWhenMarkedForFailure()
    {
        EmbeddedTransaction tx = createTransaction();

        tx.failure();

        assertTrue( tx.isOpen() );
    }

    @Test
    void shouldBeClosedAfterCommit()
    {
        EmbeddedTransaction tx = createTransaction();

        tx.success();
        tx.close();

        assertFalse( tx.isOpen() );
    }

    @Test
    void shouldBeClosedAfterRollback()
    {
        EmbeddedTransaction tx = createTransaction();

        tx.failure();
        tx.close();

        assertFalse( tx.isOpen() );
    }

    EmbeddedTransaction createTransaction()
    {
        return (EmbeddedTransaction) EmbeddedTransaction.begin( graphDatabaseService, TransactionConfig.empty(), false );
    }
}