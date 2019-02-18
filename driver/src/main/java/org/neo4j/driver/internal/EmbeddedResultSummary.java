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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import org.neo4j.driver.internal.summary.InternalInputPosition;
import org.neo4j.driver.internal.summary.InternalNotification;
import org.neo4j.driver.internal.summary.InternalSummaryCounters;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.summary.InputPosition;
import org.neo4j.driver.v1.summary.Notification;
import org.neo4j.driver.v1.summary.Plan;
import org.neo4j.driver.v1.summary.ProfiledPlan;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.ServerInfo;
import org.neo4j.driver.v1.summary.StatementType;
import org.neo4j.driver.v1.summary.SummaryCounters;
import org.neo4j.driver.v1.util.Immutable;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Result;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@Immutable
class EmbeddedResultSummary implements ResultSummary
{

    private final Statement statement;
    private final StatementType statementType;
    private final SummaryCounters summaryCounters;
    private final Plan plan;
    private final ProfiledPlan profile;
    private final List<Notification> notifications;

    static EmbeddedResultSummary extractSummary( Statement statement, Result result )
    {
        StatementType statementType = convertQueryType( result.getQueryExecutionType().queryType() );
        QueryStatistics queryStatistics = result.getQueryStatistics();
        SummaryCounters summaryCounters =
                new InternalSummaryCounters( queryStatistics.getNodesCreated(), queryStatistics.getNodesDeleted(), queryStatistics.getRelationshipsCreated(),
                        queryStatistics.getRelationshipsDeleted(), queryStatistics.getPropertiesSet(), queryStatistics.getLabelsAdded(),
                        queryStatistics.getLabelsRemoved(), queryStatistics.getIndexesAdded(), queryStatistics.getIndexesRemoved(),
                        queryStatistics.getConstraintsAdded(), queryStatistics.getConstraintsRemoved() );

        boolean hasPlan = result.getQueryExecutionType().isExplained();
        Plan plan = hasPlan ? convertExecutionPlan( result.getExecutionPlanDescription() ) : null;

        boolean hasProfile = result.getQueryExecutionType().isProfiled();
        ProfiledPlan profile = hasProfile ? (ProfiledPlan) convertExecutionPlan( result.getExecutionPlanDescription() ) : null;

        List<Notification> notifications = StreamSupport.stream( result.getNotifications().spliterator(), false ) //
                .map( EmbeddedResultSummary::convertNotification ) //
                .collect( toList() );

        return new EmbeddedResultSummary( statement, statementType, summaryCounters, plan, profile, notifications );
    }

    private EmbeddedResultSummary( Statement statement, StatementType statementType, SummaryCounters summaryCounters, Plan plan, ProfiledPlan profile,
            List<Notification> notifications )
    {
        this.statement = statement;
        this.statementType = statementType;
        this.summaryCounters = summaryCounters;
        this.plan = plan;
        this.profile = profile;
        this.notifications = notifications;
    }

    @Override
    public Statement statement()
    {
        return statement;
    }

    @Override
    public SummaryCounters counters()
    {
        return summaryCounters;
    }

    @Override
    public StatementType statementType()
    {
        return statementType;
    }

    @Override
    public boolean hasPlan()
    {
        return plan != null;
    }

    @Override
    public Plan plan()
    {
        return plan;
    }

    @Override
    public boolean hasProfile()
    {
        return false;
    }

    @Override
    public ProfiledPlan profile()
    {
        return profile;
    }

    @Override
    public List<Notification> notifications()
    {
        return notifications;
    }

    /**
     * Will always return -1.
     * TODO Discuss whether a simple time taking between calling graphDatabaseService.execute and it's return is enough to fill this value.
     *
     * @param unit The unit of the duration.
     * @return
     */
    @Override
    public long resultAvailableAfter( TimeUnit unit )
    {
        return -1;
    }

    /**
     * Will always return -1.
     *
     * @param unit The unit of the duration.
     * @return
     */
    @Override
    public long resultConsumedAfter( TimeUnit unit )
    {
        return -1;
    }

    @Override
    public ServerInfo server()
    {
        throw new UnsupportedOperationException( "Server info is not available when running the embedded driver!" );
    }

    /**
     * Converts the embedded notification into the drivers one.
     * TODO Discuss whether to extract {@code AbstractNotification} as well.
     *
     * @param embeddedNotification
     * @return
     */
    private static Notification convertNotification( org.neo4j.graphdb.Notification embeddedNotification )
    {
        return new InternalNotification( //
                embeddedNotification.getCode(), //
                embeddedNotification.getTitle(),  //
                embeddedNotification.getDescription(), //
                embeddedNotification.getSeverity().name(), //
                convertInputPosition( embeddedNotification.getPosition() ).orElse( null ) //
        );
    }

    /**
     * Converts the embedded input position to the drivers one.
     * TODO Discuss whether to extract {@code AbstractInputPosistion} as well.
     *
     * @param embeddedInputPosition
     * @return
     */
    private static Optional<InputPosition> convertInputPosition( org.neo4j.graphdb.InputPosition embeddedInputPosition )
    {
        return Optional.ofNullable( embeddedInputPosition ).map( in -> new InternalInputPosition( in.getOffset(), in.getLine(), in.getColumn() ) );
    }

    private static Plan convertExecutionPlan( ExecutionPlanDescription executionPlanDescription )
    {
        List<Plan> childPlans = Collections.emptyList();
        if ( !executionPlanDescription.getChildren().isEmpty() )
        {
            childPlans = executionPlanDescription.getChildren().stream().map( EmbeddedResultSummary::convertExecutionPlan ).collect( toList() );
        }

        String operatorType = executionPlanDescription.getName();
        Map<String,Value> arguments = convertPlanArguments( executionPlanDescription.getArguments() );
        List<String> identifiers = convertPlanIdentifiers( executionPlanDescription.getIdentifiers() );

        if ( !executionPlanDescription.hasProfilerStatistics() )
        {
            return new EmbeddedPlan<Plan>( operatorType, identifiers, arguments, childPlans );
        }
        else
        {
            List<ProfiledPlan> profiledChildPlans =
                    childPlans.stream().filter( ProfiledPlan.class::isInstance ).map( ProfiledPlan.class::cast ).collect( toList() );
            ExecutionPlanDescription.ProfilerStatistics profilerStatistics = executionPlanDescription.getProfilerStatistics();
            return new EmbeddedProfiledPlan( operatorType, identifiers, arguments, profiledChildPlans, profilerStatistics.getDbHits(),
                    profilerStatistics.getRows() );
        }
    }

    private static Map<String,Value> convertPlanArguments( Map<String,Object> planArguments )
    {
        if ( planArguments == null || planArguments.isEmpty() )
        {
            return Collections.emptyMap();
        }

        return planArguments.entrySet().stream().collect(
                collectingAndThen( toMap( e -> e.getKey(), e -> Values.value( e.getValue() ) ), Collections::unmodifiableMap ) );
    }

    private static List<String> convertPlanIdentifiers( Set<String> planIdentifiers )
    {
        if ( planIdentifiers == null || planIdentifiers.isEmpty() )
        {
            return Collections.emptyList();
        }

        return Collections.unmodifiableList( new ArrayList<>( planIdentifiers ) );
    }

    private static StatementType convertQueryType( QueryExecutionType.QueryType queryType )
    {
        switch ( queryType )
        {
        case READ_ONLY:
            return StatementType.READ_ONLY;
        case READ_WRITE:
            return StatementType.READ_WRITE;
        case WRITE:
            return StatementType.WRITE_ONLY;
        case SCHEMA_WRITE:
            return StatementType.SCHEMA_WRITE;
        default:
            throw new UnsupportedOperationException( String.format( "Cannot convert query type %s to a supported statement type", queryType.name() ) );
        }
    }

    static class EmbeddedPlan<T extends Plan> implements Plan
    {

        private final String operatorType;
        private final List<String> identifiers;
        private final Map<String,Value> arguments;
        private final List<T> children;

        public EmbeddedPlan( String operatorType, List<String> identifiers, Map<String,Value> arguments, List<T> children )
        {
            this.operatorType = operatorType;
            this.identifiers = identifiers;
            this.arguments = arguments;
            this.children = children;
        }

        @Override
        public String operatorType()
        {
            return operatorType;
        }

        @Override
        public Map<String,Value> arguments()
        {
            return arguments;
        }

        @Override
        public List<String> identifiers()
        {
            return identifiers;
        }

        @Override
        public List<T> children()
        {
            return children;
        }
    }

    static class EmbeddedProfiledPlan extends EmbeddedPlan<ProfiledPlan> implements ProfiledPlan
    {
        private final long dbHits;
        private final long records;

        public EmbeddedProfiledPlan( String operatorType, List<String> identifiers, Map<String,Value> arguments, List<ProfiledPlan> children, long dbHits,
                long records )
        {
            super( operatorType, identifiers, arguments, children );
            this.dbHits = dbHits;
            this.records = records;
        }

        @Override
        public long dbHits()
        {
            return dbHits;
        }

        @Override
        public long records()
        {
            return records;
        }
    }
}
