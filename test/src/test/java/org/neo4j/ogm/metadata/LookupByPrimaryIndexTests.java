/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This product is licensed to you under the Apache License, Version 2.0 (the "License").
 * You may not use this product except in compliance with the License.
 *
 * This product may include a number of subcomponents with
 * separate copyright notices and license terms. Your use of the source
 * code for these subcomponents is subject to the terms and
 *  conditions of the subcomponent's license, as noted in the LICENSE file.
 */

package org.neo4j.ogm.metadata;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.ogm.domain.annotations.ids.ValidAnnotations;
import org.neo4j.ogm.domain.autoindex.valid.Invoice;
import org.neo4j.ogm.domain.cineasts.annotated.ExtendedUser;
import org.neo4j.ogm.domain.cineasts.annotated.User;
import org.neo4j.ogm.domain.cineasts.partial.Actor;
import org.neo4j.ogm.session.Neo4jException;
import org.neo4j.ogm.session.Session;
import org.neo4j.ogm.session.SessionFactory;
import org.neo4j.ogm.testutil.MultiDriverTestClass;

import static java.util.Arrays.asList;

/**
 * @author Mark Angrish
 * @author Nicolas Mervaillie
 */
public class LookupByPrimaryIndexTests extends MultiDriverTestClass {

    private Session session;

    @BeforeClass
    public static void oneTimeSetUp() {
        sessionFactory = new SessionFactory(driver, "org.neo4j.ogm.domain.cineasts.annotated", "org.neo4j.ogm.domain.annotations.ids");
    }

    @Before
    public void setUp() {
        session = sessionFactory.openSession();
    }

    @Test
    public void loadUsesIdWhenPresent() {

        ValidAnnotations.Basic entity = new ValidAnnotations.Basic();
        entity.identifier = "id1";
        session.save(entity);

        final Session session2 = sessionFactory.openSession();

        final ValidAnnotations.Basic retrievedEntity = session2.load(ValidAnnotations.Basic.class, "id1");
        assertThat(retrievedEntity).isNotNull();
        assertThat(retrievedEntity.identifier).isEqualTo(entity.identifier);
    }

    @Test
    public void loadUsesIdWhenPresentOnParent() {

        ValidAnnotations.BasicChild entity = new ValidAnnotations.BasicChild();
        entity.identifier = "id1";
        session.save(entity);

        final Session session2 = sessionFactory.openSession();

        final ValidAnnotations.Basic retrievedEntity = session2.load(ValidAnnotations.Basic.class, "id1");
        assertThat(retrievedEntity).isNotNull();
        assertThat(retrievedEntity.identifier).isEqualTo(entity.identifier);
    }

    @Test
    public void saveWithStringUuidGeneration() {

        ValidAnnotations.IdAndGenerationType entity = new ValidAnnotations.IdAndGenerationType();
        session.save(entity);

        assertThat(entity.identifier).isNotNull();

        final Session session2 = sessionFactory.openSession();

        final ValidAnnotations.IdAndGenerationType retrievedEntity = session2.load(ValidAnnotations.IdAndGenerationType.class, entity.identifier);
        assertThat(retrievedEntity).isNotNull();
        assertThat(retrievedEntity.identifier).isNotNull().isEqualTo(entity.identifier);
    }

    @Test
    public void saveWithUuidGeneration() {

        ValidAnnotations.UuidIdAndGenerationType entity = new ValidAnnotations.UuidIdAndGenerationType();
        session.save(entity);

        assertThat(entity.identifier).isNotNull();

        final Session session2 = sessionFactory.openSession();

        final ValidAnnotations.UuidIdAndGenerationType retrievedEntity = session2.load(ValidAnnotations.UuidIdAndGenerationType.class, entity.identifier);
        assertThat(retrievedEntity).isNotNull();
        assertThat(retrievedEntity.identifier).isNotNull().isEqualTo(entity.identifier);
    }

    @Test
    public void loadUsesPrimaryIndexWhenPresent() {

        User user1 = new User("login1", "Name 1", "password");
        session.save(user1);

        final Session session2 = sessionFactory.openSession();

        final User retrievedUser1 = session2.load(User.class, "login1");
        assertThat(retrievedUser1).isNotNull();
        assertThat(retrievedUser1.getLogin()).isEqualTo(user1.getLogin());
    }

    @Test
    public void loadAllUsesPrimaryIndexWhenPresent() {

        User user1 = new User("login1", "Name 1", "password");
        session.save(user1);
        User user2 = new User("login2", "Name 2", "password");
        session.save(user2);

        session.clear();
        Collection<User> users = session.loadAll(User.class, asList("login1", "login2"));
        assertThat(users.size()).isEqualTo(2);

        session.clear();
        users = session.loadAll(User.class, asList("login1", "login2"), 0);
        assertThat(users.size()).isEqualTo(2);

        session.clear();
        users = session.loadAll(User.class, asList("login1", "login2"), -1);
        assertThat(users.size()).isEqualTo(2);
    }

    @Test
    public void loadUsesPrimaryIndexWhenPresentOnSuperclass() {

        ExtendedUser user1 = new ExtendedUser("login2", "Name 2", "password");
        session.save(user1);

        final Session session2 = sessionFactory.openSession();

        final User retrievedUser1 = session2.load(ExtendedUser.class, "login2");
        assertThat(retrievedUser1).isNotNull();
        assertThat(retrievedUser1.getLogin()).isEqualTo(user1.getLogin());
    }

    @Test
    public void loadUsesGraphIdWhenPrimaryIndexNotPresent() {

        SessionFactory sessionFactory = new SessionFactory(driver, "org.neo4j.ogm.domain.cineasts.partial");
        Session session1 = sessionFactory.openSession();
        Actor actor = new Actor("David Hasslehoff");
        session1.save(actor);

        final Long id = actor.getId();

        final Session session2 = sessionFactory.openSession();

        final Actor retrievedActor = session2.load(Actor.class, id);
        assertThat(retrievedActor).isNotNull();
        assertThat(retrievedActor.getName()).isEqualTo(actor.getName());
    }

    @Test(expected = Neo4jException.class)
    public void exceptionRaisedWhenLookupIsDoneWithGraphIdAndThereIsAPrimaryIndexPresent() {

        final Session session = sessionFactory.openSession();

        User user1 = new User("login1", "Name 1", "password");
        session.save(user1);

        final Session session2 = sessionFactory.openSession();

        session2.load(User.class, user1.getId());
    }

    /**
     * This test makes sure that if the primary key is a Long, it isn't mixed up with the Graph Id.
     */
    @Test
    public void loadUsesPrimaryIndexWhenPresentEvenIfTypeIsLong() {

        SessionFactory sessionFactory = new SessionFactory(driver, "org.neo4j.ogm.domain.autoindex.valid");
        Session session1 = sessionFactory.openSession();

        Invoice invoice = new Invoice(223L, "Company", 100000L);
        session1.save(invoice);

        final Session session2 = sessionFactory.openSession();

        Invoice retrievedInvoice = session2.load(Invoice.class, 223L);
        assertThat(retrievedInvoice).isNotNull();
        assertThat(retrievedInvoice.getId()).isEqualTo(invoice.getId());
    }
}