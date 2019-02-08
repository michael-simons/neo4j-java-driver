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

import org.neo4j.driver.v1.Session;

/**
 * @since 2.0
 */
enum TransactionState
{
    /** The transaction is running with no explicit success or failure marked */
    ACTIVE,

    /** Running, user marked for success, meaning it'll value committed */
    MARKED_SUCCESS,

    /** User marked as failed, meaning it'll be rolled back. */
    MARKED_FAILED,

    /**
     * This transaction has been terminated either because of explicit {@link Session#reset()} or because of a
     * fatal connection error.
     */
    TERMINATED,

    /** This transaction has successfully committed */
    COMMITTED,

    /** This transaction has been rolled back */
    ROLLED_BACK}
