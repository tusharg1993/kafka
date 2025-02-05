/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResults;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupSubscribedToTopicException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicDeletionDisabledException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResultCollection;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember;
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult;
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.requests.AlterClientQuotasResponse;
import org.apache.kafka.common.requests.AlterPartitionReassignmentsResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.CreateAclsResponse;
import org.apache.kafka.common.requests.CreateAclsResponse.AclCreationResponse;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteAclsResponse;
import org.apache.kafka.common.requests.DeleteAclsResponse.AclDeletionResult;
import org.apache.kafka.common.requests.DeleteAclsResponse.AclFilterResponse;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.DescribeClientQuotasResponse;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.ElectLeadersResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.IncrementalAlterConfigsResponse;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.ListPartitionReassignmentsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetDeleteResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse;
import static org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignableTopicResponse;
import static org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingPartitionReassignment;
import static org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingTopicReassignment;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A unit test for KafkaAdminClient.
 *
 * See AdminClientIntegrationTest for an integration test.
 */
public class KafkaAdminClientTest {
    private static final Logger log = LoggerFactory.getLogger(KafkaAdminClientTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testGetOrCreateListValue() {
        Map<String, List<String>> map = new HashMap<>();
        List<String> fooList = KafkaAdminClient.getOrCreateListValue(map, "foo");
        assertNotNull(fooList);
        fooList.add("a");
        fooList.add("b");
        List<String> fooList2 = KafkaAdminClient.getOrCreateListValue(map, "foo");
        assertEquals(fooList, fooList2);
        assertTrue(fooList2.contains("a"));
        assertTrue(fooList2.contains("b"));
        List<String> barList = KafkaAdminClient.getOrCreateListValue(map, "bar");
        assertNotNull(barList);
        assertTrue(barList.isEmpty());
    }

    @Test
    public void testCalcTimeoutMsRemainingAsInt() {
        assertEquals(0, KafkaAdminClient.calcTimeoutMsRemainingAsInt(1000, 1000));
        assertEquals(100, KafkaAdminClient.calcTimeoutMsRemainingAsInt(1000, 1100));
        assertEquals(Integer.MAX_VALUE, KafkaAdminClient.calcTimeoutMsRemainingAsInt(0, Long.MAX_VALUE));
        assertEquals(Integer.MIN_VALUE, KafkaAdminClient.calcTimeoutMsRemainingAsInt(Long.MAX_VALUE, 0));
    }

    @Test
    public void testPrettyPrintException() {
        assertEquals("Null exception.", KafkaAdminClient.prettyPrintException(null));
        assertEquals("TimeoutException", KafkaAdminClient.prettyPrintException(new TimeoutException()));
        assertEquals("TimeoutException: The foobar timed out.",
                KafkaAdminClient.prettyPrintException(new TimeoutException("The foobar timed out.")));
    }

    private static Map<String, Object> newStrMap(String... vals) {
        Map<String, Object> map = new HashMap<>();
        map.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8121");
        map.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        if (vals.length % 2 != 0) {
            throw new IllegalStateException();
        }
        for (int i = 0; i < vals.length; i += 2) {
            map.put(vals[i], vals[i + 1]);
        }
        return map;
    }

    private static AdminClientConfig newConfMap(String... vals) {
        return new AdminClientConfig(newStrMap(vals));
    }

    @Test
    public void testGenerateClientId() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String id = KafkaAdminClient.generateClientId(newConfMap(AdminClientConfig.CLIENT_ID_CONFIG, ""));
            assertTrue("Got duplicate id " + id, !ids.contains(id));
            ids.add(id);
        }
        assertEquals("myCustomId",
                KafkaAdminClient.generateClientId(newConfMap(AdminClientConfig.CLIENT_ID_CONFIG, "myCustomId")));
    }

    private static Cluster mockCluster(int controllerIndex) {
        HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));
        nodes.put(1, new Node(1, "localhost", 8122));
        nodes.put(2, new Node(2, "localhost", 8123));
        return new Cluster("mockClusterId", nodes.values(),
                Collections.emptySet(), Collections.emptySet(),
                Collections.emptySet(), nodes.get(controllerIndex));
    }

    private static Cluster mockBootstrapCluster() {
        return Cluster.bootstrap(ClientUtils.parseAndValidateAddresses(
                singletonList("localhost:8121"), ClientDnsLookup.USE_ALL_DNS_IPS));
    }

    private static AdminClientUnitTestEnv mockClientEnv(String... configVals) {
        return new AdminClientUnitTestEnv(mockCluster(0), configVals);
    }

    @Test
    public void testCloseAdminClient() {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
        }
    }

    private static OffsetDeleteResponse prepareOffsetDeleteResponse(Errors error) {
        return new OffsetDeleteResponse(
            new OffsetDeleteResponseData()
                .setErrorCode(error.code())
                .setTopics(new OffsetDeleteResponseTopicCollection())
        );
    }

    private static OffsetDeleteResponse prepareOffsetDeleteResponse(String topic, int partition, Errors error) {
        return new OffsetDeleteResponse(
            new OffsetDeleteResponseData()
                .setErrorCode(Errors.NONE.code())
                .setTopics(new OffsetDeleteResponseTopicCollection(Stream.of(
                    new OffsetDeleteResponseTopic()
                        .setName(topic)
                        .setPartitions(new OffsetDeleteResponsePartitionCollection(Collections.singletonList(
                            new OffsetDeleteResponsePartition()
                                .setPartitionIndex(partition)
                                .setErrorCode(error.code())
                        ).iterator()))
                ).collect(Collectors.toList()).iterator()))
        );
    }

    private static CreateTopicsResponse prepareCreateTopicsResponse(String topicName, Errors error) {
        CreateTopicsResponseData data = new CreateTopicsResponseData();
        data.topics().add(new CreatableTopicResult().
            setName(topicName).setErrorCode(error.code()));
        return new CreateTopicsResponse(data);
    }

    private static DeleteTopicsResponse prepareDeleteTopicsResponse(String topicName, Errors error) {
        DeleteTopicsResponseData data = new DeleteTopicsResponseData();
        data.responses().add(new DeletableTopicResult()
                .setName(topicName)
                .setErrorCode(error.code()));
        return new DeleteTopicsResponse(data);
    }

    private static FindCoordinatorResponse prepareFindCoordinatorResponse(Errors error, Node node) {
        return FindCoordinatorResponse.prepareResponse(error, node);
    }

    /**
     * Test that the client properly times out when we don't receive any metadata.
     */
    @Test
    public void testTimeoutWithoutMetadata() throws Exception {
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, mockBootstrapCluster(),
                newStrMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10"))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareCreateTopicsResponse("myTopic", Errors.NONE));
            KafkaFuture<Void> future = env.adminClient().createTopics(
                    Collections.singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(1000)).all();
            TestUtils.assertFutureError(future, TimeoutException.class);
        }
    }

    @Test
    public void testConnectionFailureOnMetadataUpdate() throws Exception {
        // This tests the scenario in which we successfully connect to the bootstrap server, but
        // the server disconnects before sending the full response

        Cluster cluster = mockBootstrapCluster();
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, cluster)) {
            Cluster discoveredCluster = mockCluster(0);
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(request -> request instanceof MetadataRequest, null, true);
            env.kafkaClient().prepareResponse(request -> request instanceof MetadataRequest,
                    MetadataResponse.prepareResponse(discoveredCluster.nodes(), discoveredCluster.clusterResource().clusterId(),
                            1, Collections.emptyList()));
            env.kafkaClient().prepareResponse(body -> body instanceof CreateTopicsRequest,
                    prepareCreateTopicsResponse("myTopic", Errors.NONE));

            KafkaFuture<Void> future = env.adminClient().createTopics(
                    Collections.singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all();

            future.get();
        }
    }

    @Test
    public void testUnreachableBootstrapServer() throws Exception {
        // This tests the scenario in which the bootstrap server is unreachable for a short while,
        // which prevents AdminClient from being able to send the initial metadata request

        Cluster cluster = Cluster.bootstrap(singletonList(new InetSocketAddress("localhost", 8121)));
        Map<Node, Long> unreachableNodes = Collections.singletonMap(cluster.nodes().get(0), 200L);
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, cluster,
                AdminClientUnitTestEnv.clientConfigs(), unreachableNodes)) {
            Cluster discoveredCluster = mockCluster(0);
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(body -> body instanceof MetadataRequest,
                    MetadataResponse.prepareResponse(discoveredCluster.nodes(), discoveredCluster.clusterResource().clusterId(),
                            1, Collections.emptyList()));
            env.kafkaClient().prepareResponse(body -> body instanceof CreateTopicsRequest,
                prepareCreateTopicsResponse("myTopic", Errors.NONE));

            KafkaFuture<Void> future = env.adminClient().createTopics(
                    Collections.singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all();

            future.get();
        }
    }

    /**
     * Test that we propagate exceptions encountered when fetching metadata.
     */
    @Test
    public void testPropagatedMetadataFetchException() throws Exception {
        Cluster cluster = mockCluster(0);
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, cluster,
                newStrMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8121",
                    AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10"))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().createPendingAuthenticationError(cluster.nodeById(0),
                    TimeUnit.DAYS.toMillis(1));
            env.kafkaClient().prepareResponse(prepareCreateTopicsResponse("myTopic", Errors.NONE));
            KafkaFuture<Void> future = env.adminClient().createTopics(
                Collections.singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                new CreateTopicsOptions().timeoutMs(1000)).all();
            TestUtils.assertFutureError(future, SaslAuthenticationException.class);
        }
    }

    @Test
    public void testCreateTopics() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(body -> body instanceof CreateTopicsRequest,
                    prepareCreateTopicsResponse("myTopic", Errors.NONE));
            KafkaFuture<Void> future = env.adminClient().createTopics(
                    Collections.singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all();
            future.get();
        }
    }

    @Test
    public void testCreateTopicsRetryBackoff() throws Exception {
        Cluster cluster = mockCluster(0);
        MockTime time = new MockTime();
        int retryBackoff = 100;

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
                newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff))) {
            MockClient mockClient = env.kafkaClient();

            mockClient.setNodeApiVersions(NodeApiVersions.create());

            AtomicLong firstAttemptTime = new AtomicLong(0);
            AtomicLong secondAttemptTime = new AtomicLong(0);

            mockClient.prepareResponse(body -> {
                firstAttemptTime.set(time.milliseconds());
                return body instanceof CreateTopicsRequest;
            }, null, true);

            mockClient.prepareResponse(body -> {
                secondAttemptTime.set(time.milliseconds());
                return body instanceof CreateTopicsRequest;
            }, prepareCreateTopicsResponse("myTopic", Errors.NONE));

            KafkaFuture<Void> future = env.adminClient().createTopics(
                    Collections.singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all();

            // Wait until the first attempt has failed, then advance the time
            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1,
                    "Failed awaiting CreateTopics first request failure");

            // Wait until the retry call added to the queue in AdminClient
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1,
                "Failed to add retry CreateTopics call");

            time.sleep(retryBackoff);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals("CreateTopics retry did not await expected backoff",
                    retryBackoff, actualRetryBackoff);
        }
    }

    @Test
    public void testCreateTopicsHandleNotControllerException() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponseFrom(
                prepareCreateTopicsResponse("myTopic", Errors.NOT_CONTROLLER),
                env.cluster().nodeById(0));
            env.kafkaClient().prepareResponse(MetadataResponse.prepareResponse(env.cluster().nodes(),
                env.cluster().clusterResource().clusterId(),
                1,
                Collections.<MetadataResponse.TopicMetadata>emptyList()));
            env.kafkaClient().prepareResponseFrom(
                prepareCreateTopicsResponse("myTopic", Errors.NONE),
                env.cluster().nodeById(1));
            KafkaFuture<Void> future = env.adminClient().createTopics(
                    Collections.singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all();
            future.get();
        }
    }

    @Test
    public void testDeleteTopics() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(body -> body instanceof DeleteTopicsRequest,
                    prepareDeleteTopicsResponse("myTopic", Errors.NONE));
            KafkaFuture<Void> future = env.adminClient().deleteTopics(singletonList("myTopic"),
                    new DeleteTopicsOptions()).all();
            future.get();

            env.kafkaClient().prepareResponse(body -> body instanceof DeleteTopicsRequest,
                    prepareDeleteTopicsResponse("myTopic", Errors.TOPIC_DELETION_DISABLED));
            future = env.adminClient().deleteTopics(singletonList("myTopic"),
                    new DeleteTopicsOptions()).all();
            TestUtils.assertFutureError(future, TopicDeletionDisabledException.class);

            env.kafkaClient().prepareResponse(body -> body instanceof DeleteTopicsRequest,
                    prepareDeleteTopicsResponse("myTopic", Errors.UNKNOWN_TOPIC_OR_PARTITION));
            future = env.adminClient().deleteTopics(singletonList("myTopic"),
                    new DeleteTopicsOptions()).all();
            TestUtils.assertFutureError(future, UnknownTopicOrPartitionException.class);
        }
    }

    @Test
    public void testInvalidTopicNames() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            List<String> sillyTopicNames = asList("", null);
            Map<String, KafkaFuture<Void>> deleteFutures = env.adminClient().deleteTopics(sillyTopicNames).values();
            for (String sillyTopicName : sillyTopicNames) {
                TestUtils.assertFutureError(deleteFutures.get(sillyTopicName), InvalidTopicException.class);
            }
            assertEquals(0, env.kafkaClient().inFlightRequestCount());

            Map<String, KafkaFuture<TopicDescription>> describeFutures =
                    env.adminClient().describeTopics(sillyTopicNames).values();
            for (String sillyTopicName : sillyTopicNames) {
                TestUtils.assertFutureError(describeFutures.get(sillyTopicName), InvalidTopicException.class);
            }
            assertEquals(0, env.kafkaClient().inFlightRequestCount());

            List<NewTopic> newTopics = new ArrayList<>();
            for (String sillyTopicName : sillyTopicNames) {
                newTopics.add(new NewTopic(sillyTopicName, 1, (short) 1));
            }

            Map<String, KafkaFuture<Void>> createFutures = env.adminClient().createTopics(newTopics).values();
            for (String sillyTopicName : sillyTopicNames) {
                TestUtils.assertFutureError(createFutures .get(sillyTopicName), InvalidTopicException.class);
            }
            assertEquals(0, env.kafkaClient().inFlightRequestCount());
        }
    }

    @Test
    public void testMetadataRetries() throws Exception {
        // We should continue retrying on metadata update failures in spite of retry configuration

        String topic = "topic";
        Cluster bootstrapCluster = Cluster.bootstrap(singletonList(new InetSocketAddress("localhost", 9999)));
        Cluster initializedCluster = mockCluster(0);

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, bootstrapCluster,
                newStrMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999",
                        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000000",
                        AdminClientConfig.RETRIES_CONFIG, "0"))) {

            // The first request fails with a disconnect
            env.kafkaClient().prepareResponse(null, true);

            // The next one succeeds and gives us the controller id
            env.kafkaClient().prepareResponse(MetadataResponse.prepareResponse(initializedCluster.nodes(),
                    initializedCluster.clusterResource().clusterId(),
                    initializedCluster.controller().id(),
                    Collections.emptyList()));

            // Then we respond to the DescribeTopic request
            Node leader = initializedCluster.nodes().get(0);
            MetadataResponse.PartitionMetadata partitionMetadata = new MetadataResponse.PartitionMetadata(
                    Errors.NONE, 0, leader, Optional.of(10), singletonList(leader),
                    singletonList(leader), singletonList(leader));
            env.kafkaClient().prepareResponse(MetadataResponse.prepareResponse(initializedCluster.nodes(),
                    initializedCluster.clusterResource().clusterId(), 1,
                    singletonList(new MetadataResponse.TopicMetadata(Errors.NONE, topic, false,
                            singletonList(partitionMetadata), MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED))));

            DescribeTopicsResult result = env.adminClient().describeTopics(Collections.singleton(topic));
            Map<String, TopicDescription> topicDescriptions = result.all().get();
            assertEquals(leader, topicDescriptions.get(topic).partitions().get(0).leader());
            assertEquals(null, topicDescriptions.get(topic).authorizedOperations());
        }
    }

    @Test
    public void testAdminClientApisAuthenticationFailure() throws Exception {
        Cluster cluster = mockBootstrapCluster();
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, cluster,
                newStrMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000"))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().createPendingAuthenticationError(cluster.nodes().get(0),
                    TimeUnit.DAYS.toMillis(1));
            callAdminClientApisAndExpectAnAuthenticationError(env);
            callClientQuotasApisAndExpectAnAuthenticationError(env);
        }
    }

    private void callAdminClientApisAndExpectAnAuthenticationError(AdminClientUnitTestEnv env) throws InterruptedException {
        try {
            env.adminClient().createTopics(
                    Collections.singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all().get();
            fail("Expected an authentication error.");
        } catch (ExecutionException e) {
            assertTrue("Expected an authentication error, but got " + Utils.stackTrace(e),
                e.getCause() instanceof AuthenticationException);
        }

        try {
            Map<String, NewPartitions> counts = new HashMap<>();
            counts.put("my_topic", NewPartitions.increaseTo(3));
            counts.put("other_topic", NewPartitions.increaseTo(3, asList(asList(2), asList(3))));
            env.adminClient().createPartitions(counts).all().get();
            fail("Expected an authentication error.");
        } catch (ExecutionException e) {
            assertTrue("Expected an authentication error, but got " + Utils.stackTrace(e),
                e.getCause() instanceof AuthenticationException);
        }

        try {
            env.adminClient().createAcls(asList(ACL1, ACL2)).all().get();
            fail("Expected an authentication error.");
        } catch (ExecutionException e) {
            assertTrue("Expected an authentication error, but got " + Utils.stackTrace(e),
                e.getCause() instanceof AuthenticationException);
        }

        try {
            env.adminClient().describeAcls(FILTER1).values().get();
            fail("Expected an authentication error.");
        } catch (ExecutionException e) {
            assertTrue("Expected an authentication error, but got " + Utils.stackTrace(e),
                e.getCause() instanceof AuthenticationException);
        }

        try {
            env.adminClient().deleteAcls(asList(FILTER1, FILTER2)).all().get();
            fail("Expected an authentication error.");
        } catch (ExecutionException e) {
            assertTrue("Expected an authentication error, but got " + Utils.stackTrace(e),
                e.getCause() instanceof AuthenticationException);
        }

        try {
            env.adminClient().describeConfigs(Collections.singleton(new ConfigResource(ConfigResource.Type.BROKER, "0"))).all().get();
            fail("Expected an authentication error.");
        } catch (ExecutionException e) {
            assertTrue("Expected an authentication error, but got " + Utils.stackTrace(e),
                e.getCause() instanceof AuthenticationException);
        }
    }

    private void callClientQuotasApisAndExpectAnAuthenticationError(AdminClientUnitTestEnv env) throws InterruptedException {
        try {
            env.adminClient().describeClientQuotas(ClientQuotaFilter.all()).entities().get();
            fail("Expected an authentication error.");
        } catch (ExecutionException e) {
            assertTrue("Expected an authentication error, but got " + Utils.stackTrace(e),
                e.getCause() instanceof AuthenticationException);
        }

        try {
            ClientQuotaEntity entity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, "user"));
            ClientQuotaAlteration alteration = new ClientQuotaAlteration(entity, asList(new ClientQuotaAlteration.Op("consumer_byte_rate", 1000.0)));
            env.adminClient().alterClientQuotas(asList(alteration)).all().get();
            fail("Expected an authentication error.");
        } catch (ExecutionException e) {
            assertTrue("Expected an authentication error, but got " + Utils.stackTrace(e),
                e.getCause() instanceof AuthenticationException);
        }
    }

    private static final AclBinding ACL1 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW));
    private static final AclBinding ACL2 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic4", PatternType.LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.DENY));
    private static final AclBindingFilter FILTER1 = new AclBindingFilter(new ResourcePatternFilter(ResourceType.ANY, null, PatternType.LITERAL),
        new AccessControlEntryFilter("User:ANONYMOUS", null, AclOperation.ANY, AclPermissionType.ANY));
    private static final AclBindingFilter FILTER2 = new AclBindingFilter(new ResourcePatternFilter(ResourceType.ANY, null, PatternType.LITERAL),
        new AccessControlEntryFilter("User:bob", null, AclOperation.ANY, AclPermissionType.ANY));
    private static final AclBindingFilter UNKNOWN_FILTER = new AclBindingFilter(
        new ResourcePatternFilter(ResourceType.UNKNOWN, null, PatternType.LITERAL),
        new AccessControlEntryFilter("User:bob", null, AclOperation.ANY, AclPermissionType.ANY));

    @Test
    public void testDescribeAcls() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test a call where we get back ACL1 and ACL2.
            env.kafkaClient().prepareResponse(new DescribeAclsResponse(0, ApiError.NONE,
                    asList(ACL1, ACL2)));
            assertCollectionIs(env.adminClient().describeAcls(FILTER1).values().get(), ACL1, ACL2);

            // Test a call where we get back no results.
            env.kafkaClient().prepareResponse(new DescribeAclsResponse(0, ApiError.NONE,
                Collections.<AclBinding>emptySet()));
            assertTrue(env.adminClient().describeAcls(FILTER2).values().get().isEmpty());

            // Test a call where we get back an error.
            env.kafkaClient().prepareResponse(new DescribeAclsResponse(0,
                new ApiError(Errors.SECURITY_DISABLED, "Security is disabled"), Collections.<AclBinding>emptySet()));
            TestUtils.assertFutureError(env.adminClient().describeAcls(FILTER2).values(), SecurityDisabledException.class);

            // Test a call where we supply an invalid filter.
            TestUtils.assertFutureError(env.adminClient().describeAcls(UNKNOWN_FILTER).values(),
                InvalidRequestException.class);
        }
    }

    @Test
    public void testCreateAcls() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test a call where we successfully create two ACLs.
            env.kafkaClient().prepareResponse(new CreateAclsResponse(0,
                asList(new AclCreationResponse(ApiError.NONE), new AclCreationResponse(ApiError.NONE))));
            CreateAclsResult results = env.adminClient().createAcls(asList(ACL1, ACL2));
            assertCollectionIs(results.values().keySet(), ACL1, ACL2);
            for (KafkaFuture<Void> future : results.values().values())
                future.get();
            results.all().get();

            // Test a call where we fail to create one ACL.
            env.kafkaClient().prepareResponse(new CreateAclsResponse(0, asList(
                new AclCreationResponse(new ApiError(Errors.SECURITY_DISABLED, "Security is disabled")),
                new AclCreationResponse(ApiError.NONE))
            ));
            results = env.adminClient().createAcls(asList(ACL1, ACL2));
            assertCollectionIs(results.values().keySet(), ACL1, ACL2);
            TestUtils.assertFutureError(results.values().get(ACL1), SecurityDisabledException.class);
            results.values().get(ACL2).get();
            TestUtils.assertFutureError(results.all(), SecurityDisabledException.class);
        }
    }

    @Test
    public void testDeleteAcls() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test a call where one filter has an error.
            env.kafkaClient().prepareResponse(new DeleteAclsResponse(0, asList(
                    new AclFilterResponse(asList(new AclDeletionResult(ACL1), new AclDeletionResult(ACL2))),
                    new AclFilterResponse(new ApiError(Errors.SECURITY_DISABLED, "No security"),
                            Collections.<AclDeletionResult>emptySet()))));
            DeleteAclsResult results = env.adminClient().deleteAcls(asList(FILTER1, FILTER2));
            Map<AclBindingFilter, KafkaFuture<FilterResults>> filterResults = results.values();
            FilterResults filter1Results = filterResults.get(FILTER1).get();
            assertEquals(null, filter1Results.values().get(0).exception());
            assertEquals(ACL1, filter1Results.values().get(0).binding());
            assertEquals(null, filter1Results.values().get(1).exception());
            assertEquals(ACL2, filter1Results.values().get(1).binding());
            TestUtils.assertFutureError(filterResults.get(FILTER2), SecurityDisabledException.class);
            TestUtils.assertFutureError(results.all(), SecurityDisabledException.class);

            // Test a call where one deletion result has an error.
            env.kafkaClient().prepareResponse(new DeleteAclsResponse(0, asList(
                    new AclFilterResponse(asList(new AclDeletionResult(ACL1),
                            new AclDeletionResult(new ApiError(Errors.SECURITY_DISABLED, "No security"), ACL2))),
                    new AclFilterResponse(Collections.<AclDeletionResult>emptySet()))));
            results = env.adminClient().deleteAcls(asList(FILTER1, FILTER2));
            assertTrue(results.values().get(FILTER2).get().values().isEmpty());
            TestUtils.assertFutureError(results.all(), SecurityDisabledException.class);

            // Test a call where there are no errors.
            env.kafkaClient().prepareResponse(new DeleteAclsResponse(0, asList(
                    new AclFilterResponse(asList(new AclDeletionResult(ACL1))),
                    new AclFilterResponse(asList(new AclDeletionResult(ACL2))))));
            results = env.adminClient().deleteAcls(asList(FILTER1, FILTER2));
            Collection<AclBinding> deleted = results.all().get();
            assertCollectionIs(deleted, ACL1, ACL2);
        }
    }

    @Test
    public void testElectLeaders()  throws Exception {
        TopicPartition topic1 = new TopicPartition("topic", 0);
        TopicPartition topic2 = new TopicPartition("topic", 2);
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            for (ElectionType electionType : ElectionType.values()) {
                env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

                // Test a call where one partition has an error.
                ApiError value = ApiError.fromThrowable(new ClusterAuthorizationException(null));
                List<ReplicaElectionResult> electionResults = new ArrayList<>();
                ReplicaElectionResult electionResult = new ReplicaElectionResult();
                electionResult.setTopic(topic1.topic());
                // Add partition 1 result
                PartitionResult partition1Result = new PartitionResult();
                partition1Result.setPartitionId(topic1.partition());
                partition1Result.setErrorCode(value.error().code());
                partition1Result.setErrorMessage(value.message());
                electionResult.partitionResult().add(partition1Result);

                // Add partition 2 result
                PartitionResult partition2Result = new PartitionResult();
                partition2Result.setPartitionId(topic2.partition());
                partition2Result.setErrorCode(value.error().code());
                partition2Result.setErrorMessage(value.message());
                electionResult.partitionResult().add(partition2Result);

                electionResults.add(electionResult);

                env.kafkaClient().prepareResponse(new ElectLeadersResponse(0, Errors.NONE.code(), electionResults));
                ElectLeadersResult results = env.adminClient().electLeaders(
                        electionType,
                        new HashSet<>(asList(topic1, topic2)));
                assertEquals(results.partitions().get().get(topic2).get().getClass(), ClusterAuthorizationException.class);

                // Test a call where there are no errors. By mutating the internal of election results
                partition1Result.setErrorCode(ApiError.NONE.error().code());
                partition1Result.setErrorMessage(ApiError.NONE.message());

                partition2Result.setErrorCode(ApiError.NONE.error().code());
                partition2Result.setErrorMessage(ApiError.NONE.message());

                env.kafkaClient().prepareResponse(new ElectLeadersResponse(0, Errors.NONE.code(), electionResults));
                results = env.adminClient().electLeaders(electionType, new HashSet<>(asList(topic1, topic2)));
                assertFalse(results.partitions().get().get(topic1).isPresent());
                assertFalse(results.partitions().get().get(topic2).isPresent());

                // Now try a timeout
                results = env.adminClient().electLeaders(
                        electionType,
                        new HashSet<>(asList(topic1, topic2)),
                        new ElectLeadersOptions().timeoutMs(100));
                TestUtils.assertFutureError(results.partitions(), TimeoutException.class);
            }
        }
    }

    /**
     * Test handling timeouts.
     */
    @Ignore // The test is flaky. Should be renabled when this JIRA is fixed: https://issues.apache.org/jira/browse/KAFKA-5792
    @Test
    public void testHandleTimeout() throws Exception {
        HashMap<Integer, Node> nodes = new HashMap<>();
        MockTime time = new MockTime();
        nodes.put(0, new Node(0, "localhost", 8121));
        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
            Collections.<PartitionInfo>emptySet(), Collections.<String>emptySet(),
            Collections.<String>emptySet(), nodes.get(0));
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "1",
                AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG, "1")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            assertEquals(time, env.time());
            assertEquals(env.time(), ((KafkaAdminClient) env.adminClient()).time());

            // Make a request with an extremely short timeout.
            // Then wait for it to fail by not supplying any response.
            log.info("Starting AdminClient#listTopics...");
            final ListTopicsResult result = env.adminClient().listTopics(new ListTopicsOptions().timeoutMs(1000));
            TestUtils.waitForCondition(new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return env.kafkaClient().hasInFlightRequests();
                }
            }, "Timed out waiting for inFlightRequests");
            time.sleep(5000);
            TestUtils.waitForCondition(new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return result.listings().isDone();
                }
            }, "Timed out waiting for listTopics to complete");
            TestUtils.assertFutureError(result.listings(), TimeoutException.class);
            log.info("Verified the error result of AdminClient#listTopics");

            // The next request should succeed.
            time.sleep(5000);
            env.kafkaClient().prepareResponse(new DescribeConfigsResponse(0,
                Collections.singletonMap(new ConfigResource(ConfigResource.Type.TOPIC, "foo"),
                    new DescribeConfigsResponse.Config(ApiError.NONE,
                        Collections.emptySet()))));
            DescribeConfigsResult result2 = env.adminClient().describeConfigs(Collections.singleton(
                new ConfigResource(ConfigResource.Type.TOPIC, "foo")));
            time.sleep(5000);
            result2.values().get(new ConfigResource(ConfigResource.Type.TOPIC, "foo")).get();
        }
    }

    @Test
    public void testDescribeConfigs() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(new DescribeConfigsResponse(0,
                Collections.singletonMap(new ConfigResource(ConfigResource.Type.BROKER, "0"),
                    new DescribeConfigsResponse.Config(ApiError.NONE, Collections.emptySet()))));
            DescribeConfigsResult result2 = env.adminClient().describeConfigs(Collections.singleton(
                new ConfigResource(ConfigResource.Type.BROKER, "0")));
            result2.all().get();
        }
    }

    @Test
    public void testCreatePartitions() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            Map<String, ApiError> m = new HashMap<>();
            m.put("my_topic", ApiError.NONE);
            m.put("other_topic", ApiError.fromThrowable(new InvalidTopicException("some detailed reason")));

            // Test a call where one filter has an error.
            env.kafkaClient().prepareResponse(new CreatePartitionsResponse(0, m));

            Map<String, NewPartitions> counts = new HashMap<>();
            counts.put("my_topic", NewPartitions.increaseTo(3));
            counts.put("other_topic", NewPartitions.increaseTo(3, asList(asList(2), asList(3))));

            CreatePartitionsResult results = env.adminClient().createPartitions(counts);
            Map<String, KafkaFuture<Void>> values = results.values();
            KafkaFuture<Void> myTopicResult = values.get("my_topic");
            myTopicResult.get();
            KafkaFuture<Void> otherTopicResult = values.get("other_topic");
            try {
                otherTopicResult.get();
                fail("get() should throw ExecutionException");
            } catch (ExecutionException e0) {
                assertTrue(e0.getCause() instanceof InvalidTopicException);
                InvalidTopicException e = (InvalidTopicException) e0.getCause();
                assertEquals("some detailed reason", e.getMessage());
            }
        }
    }

    @Test
    public void testDeleteRecordsTopicAuthorizationError() {
        String topic = "foo";
        TopicPartition partition = new TopicPartition(topic, 0);

        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            List<MetadataResponse.TopicMetadata> topics = new ArrayList<>();
            topics.add(new MetadataResponse.TopicMetadata(Errors.TOPIC_AUTHORIZATION_FAILED, topic, false,
                    Collections.emptyList()));

            env.kafkaClient().prepareResponse(MetadataResponse.prepareResponse(env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(), env.cluster().controller().id(), topics));

            Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
            recordsToDelete.put(partition, RecordsToDelete.beforeOffset(10L));
            DeleteRecordsResult results = env.adminClient().deleteRecords(recordsToDelete);

            TestUtils.assertFutureThrows(results.lowWatermarks().get(partition), TopicAuthorizationException.class);
        }
    }

    @Test
    public void testDeleteRecordsMultipleSends() throws Exception {
        String topic = "foo";
        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);

        Cluster cluster = mockCluster(0);
        MockTime time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster)) {
            List<Node> nodes = cluster.nodes();

            List<MetadataResponse.PartitionMetadata> partitionMetadata = new ArrayList<>();
            partitionMetadata.add(new MetadataResponse.PartitionMetadata(Errors.NONE, tp0.partition(), nodes.get(0),
                    Optional.of(5), singletonList(nodes.get(0)), singletonList(nodes.get(0)),
                    Collections.emptyList()));
            partitionMetadata.add(new MetadataResponse.PartitionMetadata(Errors.NONE, tp1.partition(), nodes.get(1),
                    Optional.of(5), singletonList(nodes.get(1)), singletonList(nodes.get(1)), Collections.emptyList()));

            List<MetadataResponse.TopicMetadata> topicMetadata = new ArrayList<>();
            topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, topic, false, partitionMetadata));

            env.kafkaClient().prepareResponse(MetadataResponse.prepareResponse(cluster.nodes(),
                    cluster.clusterResource().clusterId(), cluster.controller().id(), topicMetadata));

            Map<TopicPartition, DeleteRecordsResponse.PartitionResponse> deletedPartitions = new HashMap<>();
            deletedPartitions.put(tp0, new DeleteRecordsResponse.PartitionResponse(3, Errors.NONE));
            env.kafkaClient().prepareResponseFrom(new DeleteRecordsResponse(0, deletedPartitions), nodes.get(0));

            env.kafkaClient().disconnect(nodes.get(1).idString());
            env.kafkaClient().createPendingAuthenticationError(nodes.get(1), 100);

            Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
            recordsToDelete.put(tp0, RecordsToDelete.beforeOffset(10L));
            recordsToDelete.put(tp1, RecordsToDelete.beforeOffset(10L));
            DeleteRecordsResult results = env.adminClient().deleteRecords(recordsToDelete);

            assertEquals(3L, results.lowWatermarks().get(tp0).get().lowWatermark());
            TestUtils.assertFutureThrows(results.lowWatermarks().get(tp1), AuthenticationException.class);
        }
    }

    @Test
    public void testDeleteRecords() throws Exception {

        HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        partitionInfos.add(new PartitionInfo("my_topic", 0, nodes.get(0), new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        partitionInfos.add(new PartitionInfo("my_topic", 1, nodes.get(0), new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        partitionInfos.add(new PartitionInfo("my_topic", 2, null, new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        partitionInfos.add(new PartitionInfo("my_topic", 3, nodes.get(0), new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        partitionInfos.add(new PartitionInfo("my_topic", 4, nodes.get(0), new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
                partitionInfos, Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));

        TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
        TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
        TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);
        TopicPartition myTopicPartition3 = new TopicPartition("my_topic", 3);
        TopicPartition myTopicPartition4 = new TopicPartition("my_topic", 4);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            Map<TopicPartition, DeleteRecordsResponse.PartitionResponse> m = new HashMap<>();
            m.put(myTopicPartition0,
                    new DeleteRecordsResponse.PartitionResponse(3, Errors.NONE));
            m.put(myTopicPartition1,
                    new DeleteRecordsResponse.PartitionResponse(DeleteRecordsResponse.INVALID_LOW_WATERMARK, Errors.OFFSET_OUT_OF_RANGE));
            m.put(myTopicPartition3,
                    new DeleteRecordsResponse.PartitionResponse(DeleteRecordsResponse.INVALID_LOW_WATERMARK, Errors.NOT_LEADER_FOR_PARTITION));
            m.put(myTopicPartition4,
                    new DeleteRecordsResponse.PartitionResponse(DeleteRecordsResponse.INVALID_LOW_WATERMARK, Errors.UNKNOWN_TOPIC_OR_PARTITION));

            List<MetadataResponse.TopicMetadata> t = new ArrayList<>();
            List<MetadataResponse.PartitionMetadata> p = new ArrayList<>();
            p.add(new MetadataResponse.PartitionMetadata(Errors.NONE, 0, nodes.get(0), Optional.of(5),
                    singletonList(nodes.get(0)), singletonList(nodes.get(0)), Collections.emptyList()));
            p.add(new MetadataResponse.PartitionMetadata(Errors.NONE, 1, nodes.get(0), Optional.of(5),
                    singletonList(nodes.get(0)), singletonList(nodes.get(0)), Collections.emptyList()));
            p.add(new MetadataResponse.PartitionMetadata(Errors.LEADER_NOT_AVAILABLE, 2, null,
                    Optional.empty(), singletonList(nodes.get(0)), singletonList(nodes.get(0)),
                    Collections.emptyList()));
            p.add(new MetadataResponse.PartitionMetadata(Errors.NONE, 3, nodes.get(0), Optional.of(5),
                    singletonList(nodes.get(0)), singletonList(nodes.get(0)), Collections.emptyList()));
            p.add(new MetadataResponse.PartitionMetadata(Errors.NONE, 4, nodes.get(0), Optional.of(5),
                    singletonList(nodes.get(0)), singletonList(nodes.get(0)), Collections.emptyList()));

            t.add(new MetadataResponse.TopicMetadata(Errors.NONE, "my_topic", false, p));

            env.kafkaClient().prepareResponse(MetadataResponse.prepareResponse(cluster.nodes(), cluster.clusterResource().clusterId(), cluster.controller().id(), t));
            env.kafkaClient().prepareResponse(new DeleteRecordsResponse(0, m));

            Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
            recordsToDelete.put(myTopicPartition0, RecordsToDelete.beforeOffset(3L));
            recordsToDelete.put(myTopicPartition1, RecordsToDelete.beforeOffset(10L));
            recordsToDelete.put(myTopicPartition2, RecordsToDelete.beforeOffset(10L));
            recordsToDelete.put(myTopicPartition3, RecordsToDelete.beforeOffset(10L));
            recordsToDelete.put(myTopicPartition4, RecordsToDelete.beforeOffset(10L));

            DeleteRecordsResult results = env.adminClient().deleteRecords(recordsToDelete);

            // success on records deletion for partition 0
            Map<TopicPartition, KafkaFuture<DeletedRecords>> values = results.lowWatermarks();
            KafkaFuture<DeletedRecords> myTopicPartition0Result = values.get(myTopicPartition0);
            long lowWatermark = myTopicPartition0Result.get().lowWatermark();
            assertEquals(lowWatermark, 3);

            // "offset out of range" failure on records deletion for partition 1
            KafkaFuture<DeletedRecords> myTopicPartition1Result = values.get(myTopicPartition1);
            try {
                myTopicPartition1Result.get();
                fail("get() should throw ExecutionException");
            } catch (ExecutionException e0) {
                assertTrue(e0.getCause() instanceof OffsetOutOfRangeException);
            }

            // "leader not available" failure on metadata request for partition 2
            KafkaFuture<DeletedRecords> myTopicPartition2Result = values.get(myTopicPartition2);
            try {
                myTopicPartition2Result.get();
                fail("get() should throw ExecutionException");
            } catch (ExecutionException e1) {
                assertTrue(e1.getCause() instanceof LeaderNotAvailableException);
            }

            // "not leader for partition" failure on records deletion for partition 3
            KafkaFuture<DeletedRecords> myTopicPartition3Result = values.get(myTopicPartition3);
            try {
                myTopicPartition3Result.get();
                fail("get() should throw ExecutionException");
            } catch (ExecutionException e1) {
                assertTrue(e1.getCause() instanceof NotLeaderForPartitionException);
            }

            // "unknown topic or partition" failure on records deletion for partition 4
            KafkaFuture<DeletedRecords> myTopicPartition4Result = values.get(myTopicPartition4);
            try {
                myTopicPartition4Result.get();
                fail("get() should throw ExecutionException");
            } catch (ExecutionException e1) {
                assertTrue(e1.getCause() instanceof UnknownTopicOrPartitionException);
            }
        }
    }

    @Test
    public void testDescribeCluster() throws Exception {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        Node node0 = new Node(0, "localhost", 8121);
        Node node1 = new Node(1, "localhost", 8122);
        Node node2 = new Node(2, "localhost", 8123);
        Node node3 = new Node(3, "localhost", 8124);
        nodes.put(0, node0);
        nodes.put(1, node1);
        nodes.put(2, node2);
        nodes.put(3, node3);

        final Cluster cluster = new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.emptyList(),
                Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster, AdminClientConfig.RETRIES_CONFIG, "2")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Prepare the metadata response used for the first describe cluster
            MetadataResponse response = MetadataResponse.prepareResponse(0,
                    new ArrayList<>(nodes.values()),
                    env.cluster().clusterResource().clusterId(),
                    2,
                    Collections.emptyList(),
                    MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED);
            env.kafkaClient().prepareResponse(response);

            // Prepare the metadata response used for the second describe cluster
            MetadataResponse response2 = MetadataResponse.prepareResponse(0,
                    new ArrayList<>(nodes.values()),
                    env.cluster().clusterResource().clusterId(),
                    3,
                    Collections.emptyList(),
                    1 << AclOperation.DESCRIBE.code() | 1 << AclOperation.ALTER.code());
            env.kafkaClient().prepareResponse(response2);

            // Test DescribeCluster with the authorized operations omitted.
            final DescribeClusterResult result = env.adminClient().describeCluster();
            assertEquals(env.cluster().clusterResource().clusterId(), result.clusterId().get());
            assertEquals(2, result.controller().get().id());
            assertEquals(null, result.authorizedOperations().get());

            // Test DescribeCluster with the authorized operations included.
            final DescribeClusterResult result2 = env.adminClient().describeCluster();
            assertEquals(env.cluster().clusterResource().clusterId(), result2.clusterId().get());
            assertEquals(3, result2.controller().get().id());
            assertEquals(new HashSet<>(Arrays.asList(AclOperation.DESCRIBE, AclOperation.ALTER)),
                    result2.authorizedOperations().get());
        }
    }

    @Test
    public void testListConsumerGroups() throws Exception {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        Node node0 = new Node(0, "localhost", 8121);
        Node node1 = new Node(1, "localhost", 8122);
        Node node2 = new Node(2, "localhost", 8123);
        Node node3 = new Node(3, "localhost", 8124);
        nodes.put(0, node0);
        nodes.put(1, node1);
        nodes.put(2, node2);
        nodes.put(3, node3);

        final Cluster cluster = new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.emptyList(),
                Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster, AdminClientConfig.RETRIES_CONFIG, "2")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Empty metadata response should be retried
            env.kafkaClient().prepareResponse(
                     MetadataResponse.prepareResponse(
                            Collections.emptyList(),
                            env.cluster().clusterResource().clusterId(),
                            -1,
                            Collections.emptyList()));

            env.kafkaClient().prepareResponse(
                     MetadataResponse.prepareResponse(
                            env.cluster().nodes(),
                            env.cluster().clusterResource().clusterId(),
                            env.cluster().controller().id(),
                            Collections.emptyList()));

            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                            new ListGroupsResponseData()
                            .setErrorCode(Errors.NONE.code())
                            .setGroups(Arrays.asList(
                                    new ListGroupsResponseData.ListedGroup()
                                            .setGroupId("group-1")
                                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE),
                                    new ListGroupsResponseData.ListedGroup()
                                            .setGroupId("group-connect-1")
                                            .setProtocolType("connector")
                            ))),
                    node0);

            // handle retriable errors
            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                            new ListGroupsResponseData()
                                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                                    .setGroups(Collections.emptyList())
                    ),
                    node1);
            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                            new ListGroupsResponseData()
                                    .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
                                    .setGroups(Collections.emptyList())
                    ),
                    node1);
            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                            new ListGroupsResponseData()
                                    .setErrorCode(Errors.NONE.code())
                                    .setGroups(Arrays.asList(
                                            new ListGroupsResponseData.ListedGroup()
                                                    .setGroupId("group-2")
                                                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE),
                                            new ListGroupsResponseData.ListedGroup()
                                                    .setGroupId("group-connect-2")
                                                    .setProtocolType("connector")
                            ))),
                    node1);

            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                            new ListGroupsResponseData()
                                    .setErrorCode(Errors.NONE.code())
                                    .setGroups(Arrays.asList(
                                            new ListGroupsResponseData.ListedGroup()
                                                    .setGroupId("group-3")
                                                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE),
                                            new ListGroupsResponseData.ListedGroup()
                                                    .setGroupId("group-connect-3")
                                                    .setProtocolType("connector")
                                    ))),
                    node2);

            // fatal error
            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                            new ListGroupsResponseData()
                                    .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                                    .setGroups(Collections.emptyList())),
                    node3);

            final ListConsumerGroupsResult result = env.adminClient().listConsumerGroups();
            TestUtils.assertFutureError(result.all(), UnknownServerException.class);

            Collection<ConsumerGroupListing> listings = result.valid().get();
            assertEquals(3, listings.size());

            Set<String> groupIds = new HashSet<>();
            for (ConsumerGroupListing listing : listings) {
                groupIds.add(listing.groupId());
            }

            assertEquals(Utils.mkSet("group-1", "group-2", "group-3"), groupIds);
            assertEquals(1, result.errors().get().size());
        }
    }

    @Test
    public void testListConsumerGroupsMetadataFailure() throws Exception {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        Node node0 = new Node(0, "localhost", 8121);
        Node node1 = new Node(1, "localhost", 8122);
        Node node2 = new Node(2, "localhost", 8123);
        nodes.put(0, node0);
        nodes.put(1, node1);
        nodes.put(2, node2);

        final Cluster cluster = new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.emptyList(),
                Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
                AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Empty metadata causes the request to fail since we have no list of brokers
            // to send the ListGroups requests to
            env.kafkaClient().prepareResponse(
                     MetadataResponse.prepareResponse(
                            Collections.emptyList(),
                            env.cluster().clusterResource().clusterId(),
                            -1,
                            Collections.emptyList()));

            final ListConsumerGroupsResult result = env.adminClient().listConsumerGroups();
            TestUtils.assertFutureError(result.all(), KafkaException.class);
        }
    }

    @Test
    public void testDescribeConsumerGroups() throws Exception {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));

        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.<PartitionInfo>emptyList(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            //Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE,  Node.noNode()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS,  Node.noNode()));

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            DescribeGroupsResponseData data = new DescribeGroupsResponseData();

            //Retriable errors should be retried
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                "group-0",
                Errors.COORDINATOR_LOAD_IN_PROGRESS,
                "",
                "",
                "",
                Collections.emptyList(),
                Collections.emptySet()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            data = new DescribeGroupsResponseData();
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                "group-0",
                Errors.COORDINATOR_NOT_AVAILABLE,
                "",
                "",
                "",
                Collections.emptyList(),
                Collections.emptySet()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            /*
             * We need to return two responses here, one with NOT_COORDINATOR error when calling describe consumer group
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             */
            data = new DescribeGroupsResponseData();
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                    "group-0",
                    Errors.NOT_COORDINATOR,
                    "",
                    "",
                    "",
                    Collections.emptyList(),
                    Collections.emptySet()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            data = new DescribeGroupsResponseData();
            TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
            TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
            TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);

            final List<TopicPartition> topicPartitions = new ArrayList<>();
            topicPartitions.add(0, myTopicPartition0);
            topicPartitions.add(1, myTopicPartition1);
            topicPartitions.add(2, myTopicPartition2);

            final ByteBuffer memberAssignment = ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(topicPartitions));
            byte[] memberAssignmentBytes = new byte[memberAssignment.remaining()];
            memberAssignment.get(memberAssignmentBytes);

            DescribedGroupMember memberOne = DescribeGroupsResponse.groupMember("0", "instance1", "clientId0", "clientHost", memberAssignmentBytes, null);
            DescribedGroupMember memberTwo = DescribeGroupsResponse.groupMember("1", "instance2", "clientId1", "clientHost", memberAssignmentBytes, null);

            List<MemberDescription> expectedMemberDescriptions = new ArrayList<>();
            expectedMemberDescriptions.add(convertToMemberDescriptions(memberOne,
                                                                       new MemberAssignment(new HashSet<>(topicPartitions))));
            expectedMemberDescriptions.add(convertToMemberDescriptions(memberTwo,
                                                                       new MemberAssignment(new HashSet<>(topicPartitions))));
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                    "group-0",
                    Errors.NONE,
                    "",
                    ConsumerProtocol.PROTOCOL_TYPE,
                    "",
                    asList(memberOne, memberTwo),
                    Collections.emptySet()));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            final DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(singletonList("group-0"));
            final ConsumerGroupDescription groupDescription = result.describedGroups().get("group-0").get();

            assertEquals(1, result.describedGroups().size());
            assertEquals("group-0", groupDescription.groupId());
            assertEquals(2, groupDescription.members().size());
            assertEquals(expectedMemberDescriptions, groupDescription.members());
        }
    }

    @Test
    public void testDescribeMultipleConsumerGroups() throws Exception {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));

        final Cluster cluster =
                new Cluster(
                        "mockClusterId",
                        nodes.values(),
                        Collections.<PartitionInfo>emptyList(),
                        Collections.<String>emptySet(),
                        Collections.<String>emptySet(), nodes.get(0));

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
            TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
            TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);

            final List<TopicPartition> topicPartitions = new ArrayList<>();
            topicPartitions.add(0, myTopicPartition0);
            topicPartitions.add(1, myTopicPartition1);
            topicPartitions.add(2, myTopicPartition2);

            final ByteBuffer memberAssignment = ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(topicPartitions));
            byte[] memberAssignmentBytes = new byte[memberAssignment.remaining()];
            memberAssignment.get(memberAssignmentBytes);

            DescribeGroupsResponseData group0Data = new DescribeGroupsResponseData();
            group0Data.groups().add(DescribeGroupsResponse.groupMetadata(
                    "group-0",
                    Errors.NONE,
                    "",
                    ConsumerProtocol.PROTOCOL_TYPE,
                    "",
                    asList(
                            DescribeGroupsResponse.groupMember("0", null, "clientId0", "clientHost", memberAssignmentBytes, null),
                            DescribeGroupsResponse.groupMember("1", null, "clientId1", "clientHost", memberAssignmentBytes, null)
                    ),
                    Collections.emptySet()));

            DescribeGroupsResponseData groupConnectData = new DescribeGroupsResponseData();
            group0Data.groups().add(DescribeGroupsResponse.groupMetadata(
                    "group-connect-0",
                    Errors.NONE,
                    "",
                    "connect",
                    "",
                    asList(
                            DescribeGroupsResponse.groupMember("0", null, "clientId0", "clientHost", memberAssignmentBytes, null),
                            DescribeGroupsResponse.groupMember("1", null, "clientId1", "clientHost", memberAssignmentBytes, null)
                    ),
                    Collections.emptySet()));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(group0Data));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(groupConnectData));

            Collection<String> groups = new HashSet<>();
            groups.add("group-0");
            groups.add("group-connect-0");
            final DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(groups);
            assertEquals(2, result.describedGroups().size());
            assertEquals(groups, result.describedGroups().keySet());
        }
    }

    @Test
    public void testDescribeConsumerGroupsWithAuthorizedOperationsOmitted() throws Exception {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));

        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.<PartitionInfo>emptyList(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, env.cluster().controller()));

            DescribeGroupsResponseData data = new DescribeGroupsResponseData();
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                "group-0",
                Errors.NONE,
                "",
                ConsumerProtocol.PROTOCOL_TYPE,
                "",
                Collections.emptyList(),
                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            final DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(singletonList("group-0"));
            final ConsumerGroupDescription groupDescription = result.describedGroups().get("group-0").get();

            assertNull(groupDescription.authorizedOperations());
        }
    }

    @Test
    public void testDescribeNonConsumerGroups() throws Exception {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));

        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.<PartitionInfo>emptyList(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            DescribeGroupsResponseData data = new DescribeGroupsResponseData();

            data.groups().add(DescribeGroupsResponse.groupMetadata(
                "group-0",
                Errors.NONE,
                "",
                "non-consumer",
                "",
                asList(),
                Collections.emptySet()));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            final DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(singletonList("group-0"));

            TestUtils.assertFutureError(result.describedGroups().get("group-0"), IllegalArgumentException.class);
        }
    }

    @Test
    public void testDescribeConsumerGroupOffsets() throws Exception {
        final HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));

        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.emptyList(),
                Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE,  Node.noNode()));

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            // Retriable errors should be retried
            env.kafkaClient().prepareResponse(new OffsetFetchResponse(Errors.COORDINATOR_NOT_AVAILABLE, Collections.emptyMap()));
            env.kafkaClient().prepareResponse(new OffsetFetchResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Collections.emptyMap()));

            /*
             * We need to return two responses here, one for NOT_COORDINATOR error when calling list consumer group offsets
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             */
            env.kafkaClient().prepareResponse(new OffsetFetchResponse(Errors.NOT_COORDINATOR, Collections.emptyMap()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
            TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
            TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);
            TopicPartition myTopicPartition3 = new TopicPartition("my_topic", 3);

            final Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData = new HashMap<>();
            responseData.put(myTopicPartition0, new OffsetFetchResponse.PartitionData(10,
                    Optional.empty(), "", Errors.NONE));
            responseData.put(myTopicPartition1, new OffsetFetchResponse.PartitionData(0,
                    Optional.empty(), "", Errors.NONE));
            responseData.put(myTopicPartition2, new OffsetFetchResponse.PartitionData(20,
                    Optional.empty(), "", Errors.NONE));
            responseData.put(myTopicPartition3, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET,
                    Optional.empty(), "", Errors.NONE));
            env.kafkaClient().prepareResponse(new OffsetFetchResponse(Errors.NONE, responseData));

            final ListConsumerGroupOffsetsResult result = env.adminClient().listConsumerGroupOffsets("group-0");
            final Map<TopicPartition, OffsetAndMetadata> partitionToOffsetAndMetadata = result.partitionsToOffsetAndMetadata().get();

            assertEquals(4, partitionToOffsetAndMetadata.size());
            assertEquals(10, partitionToOffsetAndMetadata.get(myTopicPartition0).offset());
            assertEquals(0, partitionToOffsetAndMetadata.get(myTopicPartition1).offset());
            assertEquals(20, partitionToOffsetAndMetadata.get(myTopicPartition2).offset());
            assertTrue(partitionToOffsetAndMetadata.containsKey(myTopicPartition3));
            assertNull(partitionToOffsetAndMetadata.get(myTopicPartition3));
        }
    }

    @Test
    public void testDeleteConsumerGroups() throws Exception {
        final Map<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));

        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.<PartitionInfo>emptyList(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));

        final List<String> groupIds = singletonList("group-0");

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            //Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE,  Node.noNode()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Node.noNode()));

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final DeletableGroupResultCollection validResponse = new DeletableGroupResultCollection();
            validResponse.add(new DeletableGroupResult()
                                  .setGroupId("group-0")
                                  .setErrorCode(Errors.NONE.code()));
            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(validResponse)
            ));

            final DeleteConsumerGroupsResult result = env.adminClient().deleteConsumerGroups(groupIds);

            final KafkaFuture<Void> results = result.deletedGroups().get("group-0");
            assertNull(results.get());

            //should throw error for non-retriable errors
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED,  Node.noNode()));

            final DeleteConsumerGroupsResult errorResult = env.adminClient().deleteConsumerGroups(groupIds);
            TestUtils.assertFutureError(errorResult.deletedGroups().get("group-0"), GroupAuthorizationException.class);

            //Retriable errors should be retried
            env.kafkaClient().prepareResponse(FindCoordinatorResponse.prepareResponse(Errors.NONE, env.cluster().controller()));

            final DeletableGroupResultCollection errorResponse1 = new DeletableGroupResultCollection();
            errorResponse1.add(new DeletableGroupResult()
                                   .setGroupId("group-0")
                                   .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            );
            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(errorResponse1)));

            final DeletableGroupResultCollection errorResponse2 = new DeletableGroupResultCollection();
            errorResponse2.add(new DeletableGroupResult()
                                   .setGroupId("group-0")
                                   .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
            );
            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(errorResponse2)));

            /*
             * We need to return two responses here, one for NOT_COORDINATOR call when calling delete a consumer group
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             */
            final DeletableGroupResultCollection coordinatorMoved = new DeletableGroupResultCollection();
            coordinatorMoved.add(new DeletableGroupResult()
                                     .setGroupId("UnitTestError")
                                     .setErrorCode(Errors.NOT_COORDINATOR.code())
            );
            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(coordinatorMoved)));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(validResponse)));

            final DeleteConsumerGroupsResult errorResult1 = env.adminClient().deleteConsumerGroups(groupIds);

            final KafkaFuture<Void> errorResults = errorResult1.deletedGroups().get("group-0");
            assertNull(errorResults.get());
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsets() throws Exception {
        // Happy path

        final Map<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));

        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.<PartitionInfo>emptyList(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));

        final String groupId = "group-0";
        final TopicPartition tp1 = new TopicPartition("foo", 0);
        final TopicPartition tp2 = new TopicPartition("bar", 0);
        final TopicPartition tp3 = new TopicPartition("foobar", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(new OffsetDeleteResponse(
                new OffsetDeleteResponseData()
                    .setTopics(new OffsetDeleteResponseTopicCollection(Stream.of(
                        new OffsetDeleteResponseTopic()
                            .setName("foo")
                            .setPartitions(new OffsetDeleteResponsePartitionCollection(Collections.singletonList(
                                new OffsetDeleteResponsePartition()
                                    .setPartitionIndex(0)
                                    .setErrorCode(Errors.NONE.code())
                            ).iterator())),
                        new OffsetDeleteResponseTopic()
                            .setName("bar")
                            .setPartitions(new OffsetDeleteResponsePartitionCollection(Collections.singletonList(
                                new OffsetDeleteResponsePartition()
                                    .setPartitionIndex(0)
                                    .setErrorCode(Errors.GROUP_SUBSCRIBED_TO_TOPIC.code())
                            ).iterator()))
                    ).collect(Collectors.toList()).iterator()))
                )
            );

            final DeleteConsumerGroupOffsetsResult errorResult = env.adminClient().deleteConsumerGroupOffsets(
                groupId, Stream.of(tp1, tp2).collect(Collectors.toSet()));

            assertNull(errorResult.partitionResult(tp1).get());
            TestUtils.assertFutureError(errorResult.all(), GroupSubscribedToTopicException.class);
            TestUtils.assertFutureError(errorResult.partitionResult(tp2), GroupSubscribedToTopicException.class);
            assertThrows(IllegalArgumentException.class, () -> errorResult.partitionResult(tp3));
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsRetriableErrors() throws Exception {
        // Retriable errors should be retried

        final Map<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));

        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.<PartitionInfo>emptyList(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));

        final String groupId = "group-0";
        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                FindCoordinatorResponse.prepareResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetDeleteResponse(Errors.COORDINATOR_NOT_AVAILABLE));

            env.kafkaClient().prepareResponse(
                prepareOffsetDeleteResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS));

            /*
             * We need to return two responses here, one for NOT_COORDINATOR call when calling delete a consumer group
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             */
            env.kafkaClient().prepareResponse(
                prepareOffsetDeleteResponse(Errors.NOT_COORDINATOR));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetDeleteResponse("foo", 0, Errors.NONE));

            final DeleteConsumerGroupOffsetsResult errorResult1 = env.adminClient()
                .deleteConsumerGroupOffsets(groupId, Stream.of(tp1).collect(Collectors.toSet()));

            assertNull(errorResult1.all().get());
            assertNull(errorResult1.partitionResult(tp1).get());
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsNonRetriableErrors() throws Exception {
        // Non-retriable errors throw an exception

        final Map<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));

        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.<PartitionInfo>emptyList(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));

        final String groupId = "group-0";
        final TopicPartition tp1 = new TopicPartition("foo", 0);
        final List<Errors> nonRetriableErrors = Arrays.asList(
            Errors.GROUP_AUTHORIZATION_FAILED, Errors.INVALID_GROUP_ID, Errors.GROUP_ID_NOT_FOUND);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            for (Errors error : nonRetriableErrors) {
                env.kafkaClient().prepareResponse(
                    prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

                env.kafkaClient().prepareResponse(
                    prepareOffsetDeleteResponse(error));

                DeleteConsumerGroupOffsetsResult errorResult = env.adminClient()
                    .deleteConsumerGroupOffsets(groupId, Stream.of(tp1).collect(Collectors.toSet()));

                TestUtils.assertFutureError(errorResult.all(), error.exception().getClass());
                TestUtils.assertFutureError(errorResult.partitionResult(tp1), error.exception().getClass());
            }
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsFindCoordinatorRetriableErrors() throws Exception {
        // Retriable FindCoordinatorResponse errors should be retried

        final Map<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));

        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.<PartitionInfo>emptyList(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));

        final String groupId = "group-0";
        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE,  Node.noNode()));
            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Node.noNode()));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetDeleteResponse("foo", 0, Errors.NONE));

            final DeleteConsumerGroupOffsetsResult result = env.adminClient()
                .deleteConsumerGroupOffsets(groupId, Stream.of(tp1).collect(Collectors.toSet()));

            assertNull(result.all().get());
            assertNull(result.partitionResult(tp1).get());
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsFindCoordinatorNonRetriableErrors() throws Exception {
        // Non-retriable FindCoordinatorResponse errors throw an exception

        final Map<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));

        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes.values(),
                Collections.<PartitionInfo>emptyList(),
                Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));

        final String groupId = "group-0";
        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED,  Node.noNode()));

            final DeleteConsumerGroupOffsetsResult errorResult = env.adminClient()
                .deleteConsumerGroupOffsets(groupId, Stream.of(tp1).collect(Collectors.toSet()));

            TestUtils.assertFutureError(errorResult.all(), GroupAuthorizationException.class);
            TestUtils.assertFutureError(errorResult.partitionResult(tp1), GroupAuthorizationException.class);
        }
    }

    @Test
    public void testIncrementalAlterConfigs()  throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            //test error scenarios
            IncrementalAlterConfigsResponseData responseData =  new IncrementalAlterConfigsResponseData();
            responseData.responses().add(new AlterConfigsResourceResponse()
                    .setResourceName("")
                    .setResourceType(ConfigResource.Type.BROKER.id())
                    .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())
                    .setErrorMessage("authorization error"));

            responseData.responses().add(new AlterConfigsResourceResponse()
                    .setResourceName("topic1")
                    .setResourceType(ConfigResource.Type.TOPIC.id())
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("Config value append is not allowed for config"));

            env.kafkaClient().prepareResponse(new IncrementalAlterConfigsResponse(responseData));

            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "");
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, "topic1");

            AlterConfigOp alterConfigOp1 = new AlterConfigOp(
                    new ConfigEntry("log.segment.bytes", "1073741"),
                    AlterConfigOp.OpType.SET);

            AlterConfigOp alterConfigOp2 = new AlterConfigOp(
                    new ConfigEntry("compression.type", "gzip"),
                    AlterConfigOp.OpType.APPEND);

            final Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            configs.put(brokerResource, singletonList(alterConfigOp1));
            configs.put(topicResource, singletonList(alterConfigOp2));

            AlterConfigsResult result = env.adminClient().incrementalAlterConfigs(configs);
            TestUtils.assertFutureError(result.values().get(brokerResource), ClusterAuthorizationException.class);
            TestUtils.assertFutureError(result.values().get(topicResource), InvalidRequestException.class);

            // Test a call where there are no errors.
            responseData =  new IncrementalAlterConfigsResponseData();
            responseData.responses().add(new AlterConfigsResourceResponse()
                    .setResourceName("")
                    .setResourceType(ConfigResource.Type.BROKER.id())
                    .setErrorCode(Errors.NONE.code())
                    .setErrorMessage(ApiError.NONE.message()));

            env.kafkaClient().prepareResponse(new IncrementalAlterConfigsResponse(responseData));
            env.adminClient().incrementalAlterConfigs(Collections.singletonMap(brokerResource, singletonList(alterConfigOp1))).all().get();
        }
    }

    @Test
    public void testRemoveMembersFromGroup() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            final String instanceOne = "instance-1";
            final String instanceTwo = "instance-2";
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE,  Node.noNode()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS,  Node.noNode()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            // Retriable errors should be retried
            env.kafkaClient().prepareResponse(null, true);
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(new LeaveGroupResponseData()
                                                                         .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())));
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(new LeaveGroupResponseData()
                                                                         .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())));

            // Inject a top-level non-retriable error
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(new LeaveGroupResponseData()
                                                                         .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())));

            String groupId = "groupId";
            Collection<MemberToRemove> membersToRemove = Arrays.asList(new MemberToRemove(instanceOne),
                                                                       new MemberToRemove(instanceTwo));
            final RemoveMembersFromConsumerGroupResult unknownErrorResult = env.adminClient().removeMembersFromConsumerGroup(
                groupId,
                new RemoveMembersFromConsumerGroupOptions(membersToRemove)
            );

            MemberToRemove memberOne = new MemberToRemove(instanceOne);
            MemberToRemove memberTwo = new MemberToRemove(instanceTwo);

            TestUtils.assertFutureError(unknownErrorResult.all(), UnknownServerException.class);
            TestUtils.assertFutureError(unknownErrorResult.memberResult(memberOne), UnknownServerException.class);
            TestUtils.assertFutureError(unknownErrorResult.memberResult(memberTwo), UnknownServerException.class);

            MemberResponse responseOne = new MemberResponse()
                                             .setGroupInstanceId(instanceOne)
                                             .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code());

            MemberResponse responseTwo = new MemberResponse()
                                             .setGroupInstanceId(instanceTwo)
                                             .setErrorCode(Errors.NONE.code());

            // Inject one member level error.
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(new LeaveGroupResponseData()
                                                                         .setErrorCode(Errors.NONE.code())
                                                                         .setMembers(Arrays.asList(responseOne, responseTwo))));

            final RemoveMembersFromConsumerGroupResult memberLevelErrorResult = env.adminClient().removeMembersFromConsumerGroup(
                groupId,
                new RemoveMembersFromConsumerGroupOptions(membersToRemove)
            );

            TestUtils.assertFutureError(memberLevelErrorResult.all(), UnknownMemberIdException.class);
            TestUtils.assertFutureError(memberLevelErrorResult.memberResult(memberOne), UnknownMemberIdException.class);
            assertNull(memberLevelErrorResult.memberResult(memberTwo).get());

            // Return with missing member.
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(new LeaveGroupResponseData()
                                                                         .setErrorCode(Errors.NONE.code())
                                                                         .setMembers(Collections.singletonList(responseTwo))));

            final RemoveMembersFromConsumerGroupResult missingMemberResult = env.adminClient().removeMembersFromConsumerGroup(
                groupId,
                new RemoveMembersFromConsumerGroupOptions(membersToRemove)
            );

            TestUtils.assertFutureError(missingMemberResult.all(), IllegalArgumentException.class);
            // The memberOne was not included in the response.
            TestUtils.assertFutureError(missingMemberResult.memberResult(memberOne), IllegalArgumentException.class);
            assertNull(missingMemberResult.memberResult(memberTwo).get());


            // Return with success.
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(
                    new LeaveGroupResponseData().setErrorCode(Errors.NONE.code()).setMembers(
                        Arrays.asList(responseTwo,
                                      new MemberResponse().setGroupInstanceId(instanceOne).setErrorCode(Errors.NONE.code())
                        ))
            ));

            final RemoveMembersFromConsumerGroupResult noErrorResult = env.adminClient().removeMembersFromConsumerGroup(
                groupId,
                new RemoveMembersFromConsumerGroupOptions(membersToRemove)
            );
            assertNull(noErrorResult.all().get());
            assertNull(noErrorResult.memberResult(memberOne).get());
            assertNull(noErrorResult.memberResult(memberTwo).get());
        }
    }

    @Test
    public void testAlterPartitionReassignments() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            TopicPartition tp1 = new TopicPartition("A", 0);
            TopicPartition tp2 = new TopicPartition("B", 0);
            Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments = new HashMap<>();
            reassignments.put(tp1, Optional.empty());
            reassignments.put(tp2, Optional.of(new NewPartitionReassignment(Arrays.asList(1, 2, 3))));

            // 1. server returns less responses than number of partitions we sent
            AlterPartitionReassignmentsResponseData responseData1 = new AlterPartitionReassignmentsResponseData();
            ReassignablePartitionResponse normalPartitionResponse = new ReassignablePartitionResponse().setPartitionIndex(0);
            responseData1.setResponses(Collections.singletonList(
                    new ReassignableTopicResponse()
                            .setName("A")
                            .setPartitions(Collections.singletonList(normalPartitionResponse))));
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(responseData1));
            AlterPartitionReassignmentsResult result1 = env.adminClient().alterPartitionReassignments(reassignments);
            Future future1 = result1.all();
            Future future2 = result1.values().get(tp1);
            TestUtils.assertFutureError(future1, UnknownServerException.class);
            TestUtils.assertFutureError(future2, UnknownServerException.class);

            // 2. NOT_CONTROLLER error handling
            AlterPartitionReassignmentsResponseData controllerErrResponseData =
                    new AlterPartitionReassignmentsResponseData()
                            .setErrorCode(Errors.NOT_CONTROLLER.code())
                            .setErrorMessage(Errors.NOT_CONTROLLER.message())
                            .setResponses(Arrays.asList(
                                new ReassignableTopicResponse()
                                        .setName("A")
                                        .setPartitions(Collections.singletonList(normalPartitionResponse)),
                                new ReassignableTopicResponse()
                                        .setName("B")
                                        .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            MetadataResponse controllerNodeResponse = MetadataResponse.prepareResponse(env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(), 1, Collections.emptyList());
            AlterPartitionReassignmentsResponseData normalResponse =
                    new AlterPartitionReassignmentsResponseData()
                            .setResponses(Arrays.asList(
                                    new ReassignableTopicResponse()
                                            .setName("A")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)),
                                    new ReassignableTopicResponse()
                                            .setName("B")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(controllerErrResponseData));
            env.kafkaClient().prepareResponse(controllerNodeResponse);
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(normalResponse));
            AlterPartitionReassignmentsResult controllerErrResult = env.adminClient().alterPartitionReassignments(reassignments);
            controllerErrResult.all().get();
            controllerErrResult.values().get(tp1).get();
            controllerErrResult.values().get(tp2).get();

            // 3. partition-level error
            AlterPartitionReassignmentsResponseData partitionLevelErrData =
                    new AlterPartitionReassignmentsResponseData()
                            .setResponses(Arrays.asList(
                                    new ReassignableTopicResponse()
                                            .setName("A")
                                            .setPartitions(Collections.singletonList(new ReassignablePartitionResponse()
                                                .setPartitionIndex(0).setErrorMessage(Errors.INVALID_REPLICA_ASSIGNMENT.message())
                                                .setErrorCode(Errors.INVALID_REPLICA_ASSIGNMENT.code())
                                            )),
                                    new ReassignableTopicResponse()
                                            .setName("B")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(partitionLevelErrData));
            AlterPartitionReassignmentsResult partitionLevelErrResult = env.adminClient().alterPartitionReassignments(reassignments);
            TestUtils.assertFutureError(partitionLevelErrResult.values().get(tp1), Errors.INVALID_REPLICA_ASSIGNMENT.exception().getClass());
            partitionLevelErrResult.values().get(tp2).get();

            // 4. top-level error
            AlterPartitionReassignmentsResponseData topLevelErrResponseData =
                    new AlterPartitionReassignmentsResponseData()
                            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())
                            .setErrorMessage(Errors.CLUSTER_AUTHORIZATION_FAILED.message())
                            .setResponses(Arrays.asList(
                                    new ReassignableTopicResponse()
                                            .setName("A")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)),
                                    new ReassignableTopicResponse()
                                            .setName("B")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(topLevelErrResponseData));
            AlterPartitionReassignmentsResult topLevelErrResult = env.adminClient().alterPartitionReassignments(reassignments);
            TestUtils.assertFutureError(topLevelErrResult.all(), Errors.CLUSTER_AUTHORIZATION_FAILED.exception().getClass());
            TestUtils.assertFutureError(topLevelErrResult.values().get(tp1), Errors.CLUSTER_AUTHORIZATION_FAILED.exception().getClass());
            TestUtils.assertFutureError(topLevelErrResult.values().get(tp2), Errors.CLUSTER_AUTHORIZATION_FAILED.exception().getClass());

            // 5. unrepresentable topic name error
            TopicPartition invalidTopicTP = new TopicPartition("", 0);
            TopicPartition invalidPartitionTP = new TopicPartition("ABC", -1);
            Map<TopicPartition, Optional<NewPartitionReassignment>> invalidTopicReassignments = new HashMap<>();
            invalidTopicReassignments.put(invalidPartitionTP, Optional.of(new NewPartitionReassignment(Arrays.asList(1, 2, 3))));
            invalidTopicReassignments.put(invalidTopicTP, Optional.of(new NewPartitionReassignment(Arrays.asList(1, 2, 3))));
            invalidTopicReassignments.put(tp1, Optional.of(new NewPartitionReassignment(Arrays.asList(1, 2, 3))));

            AlterPartitionReassignmentsResponseData singlePartResponseData =
                    new AlterPartitionReassignmentsResponseData()
                            .setResponses(Collections.singletonList(
                                    new ReassignableTopicResponse()
                                            .setName("A")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(singlePartResponseData));
            AlterPartitionReassignmentsResult unrepresentableTopicResult = env.adminClient().alterPartitionReassignments(invalidTopicReassignments);
            TestUtils.assertFutureError(unrepresentableTopicResult.values().get(invalidTopicTP), InvalidTopicException.class);
            TestUtils.assertFutureError(unrepresentableTopicResult.values().get(invalidPartitionTP), InvalidTopicException.class);
            unrepresentableTopicResult.values().get(tp1).get();

            // Test success scenario
            AlterPartitionReassignmentsResponseData noErrResponseData =
                    new AlterPartitionReassignmentsResponseData()
                            .setErrorCode(Errors.NONE.code())
                            .setErrorMessage(Errors.NONE.message())
                            .setResponses(Arrays.asList(
                                    new ReassignableTopicResponse()
                                            .setName("A")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)),
                                    new ReassignableTopicResponse()
                                            .setName("B")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(noErrResponseData));
            AlterPartitionReassignmentsResult noErrResult = env.adminClient().alterPartitionReassignments(reassignments);
            noErrResult.all().get();
            noErrResult.values().get(tp1).get();
            noErrResult.values().get(tp2).get();
        }
    }

    @Test
    public void testListPartitionReassignments() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            TopicPartition tp1 = new TopicPartition("A", 0);
            OngoingPartitionReassignment tp1PartitionReassignment = new OngoingPartitionReassignment()
                    .setPartitionIndex(0)
                    .setRemovingReplicas(Arrays.asList(1, 2, 3))
                    .setAddingReplicas(Arrays.asList(4, 5, 6))
                    .setReplicas(Arrays.asList(1, 2, 3, 4, 5, 6));
            OngoingTopicReassignment tp1Reassignment = new OngoingTopicReassignment().setName("A")
                    .setPartitions(Collections.singletonList(tp1PartitionReassignment));

            TopicPartition tp2 = new TopicPartition("B", 0);
            OngoingPartitionReassignment tp2PartitionReassignment = new OngoingPartitionReassignment()
                    .setPartitionIndex(0)
                    .setRemovingReplicas(Arrays.asList(1, 2, 3))
                    .setAddingReplicas(Arrays.asList(4, 5, 6))
                    .setReplicas(Arrays.asList(1, 2, 3, 4, 5, 6));
            OngoingTopicReassignment tp2Reassignment = new OngoingTopicReassignment().setName("B")
                    .setPartitions(Collections.singletonList(tp2PartitionReassignment));

            // 1. NOT_CONTROLLER error handling
            ListPartitionReassignmentsResponseData notControllerData = new ListPartitionReassignmentsResponseData()
                    .setErrorCode(Errors.NOT_CONTROLLER.code())
                    .setErrorMessage(Errors.NOT_CONTROLLER.message());
            MetadataResponse controllerNodeResponse = MetadataResponse.prepareResponse(env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(), 1, Collections.emptyList());
            ListPartitionReassignmentsResponseData reassignmentsData = new ListPartitionReassignmentsResponseData()
                    .setTopics(Arrays.asList(tp1Reassignment, tp2Reassignment));
            env.kafkaClient().prepareResponse(new ListPartitionReassignmentsResponse(notControllerData));
            env.kafkaClient().prepareResponse(controllerNodeResponse);
            env.kafkaClient().prepareResponse(new ListPartitionReassignmentsResponse(reassignmentsData));

            ListPartitionReassignmentsResult noControllerResult = env.adminClient().listPartitionReassignments();
            noControllerResult.reassignments().get(); // no error

            // 2. UNKNOWN_TOPIC_OR_EXCEPTION_ERROR
            ListPartitionReassignmentsResponseData unknownTpData = new ListPartitionReassignmentsResponseData()
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message());
            env.kafkaClient().prepareResponse(new ListPartitionReassignmentsResponse(unknownTpData));

            ListPartitionReassignmentsResult unknownTpResult = env.adminClient().listPartitionReassignments(new HashSet<>(Arrays.asList(tp1, tp2)));
            TestUtils.assertFutureError(unknownTpResult.reassignments(), UnknownTopicOrPartitionException.class);

            // 3. Success
            ListPartitionReassignmentsResponseData responseData = new ListPartitionReassignmentsResponseData()
                    .setTopics(Arrays.asList(tp1Reassignment, tp2Reassignment));
            env.kafkaClient().prepareResponse(new ListPartitionReassignmentsResponse(responseData));
            ListPartitionReassignmentsResult responseResult = env.adminClient().listPartitionReassignments();

            Map<TopicPartition, PartitionReassignment> reassignments = responseResult.reassignments().get();

            PartitionReassignment tp1Result = reassignments.get(tp1);
            assertEquals(tp1PartitionReassignment.addingReplicas(), tp1Result.addingReplicas());
            assertEquals(tp1PartitionReassignment.removingReplicas(), tp1Result.removingReplicas());
            assertEquals(tp1PartitionReassignment.replicas(), tp1Result.replicas());
            assertEquals(tp1PartitionReassignment.replicas(), tp1Result.replicas());
            PartitionReassignment tp2Result = reassignments.get(tp2);
            assertEquals(tp2PartitionReassignment.addingReplicas(), tp2Result.addingReplicas());
            assertEquals(tp2PartitionReassignment.removingReplicas(), tp2Result.removingReplicas());
            assertEquals(tp2PartitionReassignment.replicas(), tp2Result.replicas());
            assertEquals(tp2PartitionReassignment.replicas(), tp2Result.replicas());
        }
    }

    @Test
    public void testGetSubLevelError() {
        List<MemberIdentity> memberIdentities = Arrays.asList(
            new MemberIdentity().setGroupInstanceId("instance-0"),
            new MemberIdentity().setGroupInstanceId("instance-1"));
        Map<MemberIdentity, Errors> errorsMap = new HashMap<>();
        errorsMap.put(memberIdentities.get(0), Errors.NONE);
        errorsMap.put(memberIdentities.get(1), Errors.FENCED_INSTANCE_ID);
        assertEquals(IllegalArgumentException.class, KafkaAdminClient.getSubLevelError(errorsMap,
                                                                                       new MemberIdentity().setGroupInstanceId("non-exist-id"), "For unit test").getClass());
        assertNull(KafkaAdminClient.getSubLevelError(errorsMap, memberIdentities.get(0), "For unit test"));
        assertEquals(FencedInstanceIdException.class, KafkaAdminClient.getSubLevelError(
            errorsMap, memberIdentities.get(1), "For unit test").getClass());
    }

    private ClientQuotaEntity newClientQuotaEntity(String... args) {
        assertTrue(args.length % 2 == 0);

        Map<String, String> entityMap = new HashMap<>(args.length / 2);
        for (int index = 0; index < args.length; index += 2) {
            entityMap.put(args[index], args[index + 1]);
        }
        return new ClientQuotaEntity(entityMap);
    }

    @Test
    public void testDescribeClientQuotas() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            final String value = "value";

            Map<ClientQuotaEntity, Map<String, Double>> responseData = new HashMap<>();
            ClientQuotaEntity entity1 = newClientQuotaEntity(ClientQuotaEntity.USER, "user-1", ClientQuotaEntity.CLIENT_ID, value);
            ClientQuotaEntity entity2 = newClientQuotaEntity(ClientQuotaEntity.USER, "user-2", ClientQuotaEntity.CLIENT_ID, value);
            responseData.put(entity1, Collections.singletonMap("consumer_byte_rate", 10000.0));
            responseData.put(entity2, Collections.singletonMap("producer_byte_rate", 20000.0));

            env.kafkaClient().prepareResponse(new DescribeClientQuotasResponse(responseData, 0));

            ClientQuotaFilter filter = ClientQuotaFilter.contains(asList(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, value)));

            DescribeClientQuotasResult result = env.adminClient().describeClientQuotas(filter);
            Map<ClientQuotaEntity, Map<String, Double>> resultData = result.entities().get();
            assertEquals(resultData.size(), 2);
            assertTrue(resultData.containsKey(entity1));
            Map<String, Double> config1 = resultData.get(entity1);
            assertEquals(config1.size(), 1);
            assertEquals(config1.get("consumer_byte_rate"), 10000.0, 1e-6);
            assertTrue(resultData.containsKey(entity2));
            Map<String, Double> config2 = resultData.get(entity2);
            assertEquals(config2.size(), 1);
            assertEquals(config2.get("producer_byte_rate"), 20000.0, 1e-6);
        }
    }

    @Test
    public void testEqualsOfClientQuotaFilterComponent() {
        assertEquals(ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER),
            ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER));

        assertEquals(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER),
            ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER));

        // match = null is different from match = Empty
        assertNotEquals(ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER),
            ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER));

        assertEquals(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user"),
            ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user"));

        assertNotEquals(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user"),
            ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER));

        assertNotEquals(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user"),
            ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER));
    }

    public void testAlterClientQuotas() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            ClientQuotaEntity goodEntity = newClientQuotaEntity(ClientQuotaEntity.USER, "user-1");
            ClientQuotaEntity unauthorizedEntity = newClientQuotaEntity(ClientQuotaEntity.USER, "user-0");
            ClientQuotaEntity invalidEntity = newClientQuotaEntity("", "user-0");

            Map<ClientQuotaEntity, ApiError> responseData = new HashMap<>(2);
            responseData.put(goodEntity, new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, "Authorization failed"));
            responseData.put(unauthorizedEntity, new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, "Authorization failed"));
            responseData.put(invalidEntity, new ApiError(Errors.INVALID_REQUEST, "Invalid quota entity"));

            env.kafkaClient().prepareResponse(new AlterClientQuotasResponse(responseData, 0));

            List<ClientQuotaAlteration> entries = new ArrayList<>(3);
            entries.add(new ClientQuotaAlteration(goodEntity, Collections.singleton(new ClientQuotaAlteration.Op("consumer_byte_rate", 10000.0))));
            entries.add(new ClientQuotaAlteration(unauthorizedEntity, Collections.singleton(new ClientQuotaAlteration.Op("producer_byte_rate", 10000.0))));
            entries.add(new ClientQuotaAlteration(invalidEntity, Collections.singleton(new ClientQuotaAlteration.Op("producer_byte_rate", 100.0))));

            AlterClientQuotasResult result = env.adminClient().alterClientQuotas(entries);
            result.values().get(goodEntity);
            TestUtils.assertFutureError(result.values().get(unauthorizedEntity), ClusterAuthorizationException.class);
            TestUtils.assertFutureError(result.values().get(invalidEntity), InvalidRequestException.class);
        }
    }

    private static MemberDescription convertToMemberDescriptions(DescribedGroupMember member,
                                                                 MemberAssignment assignment) {
        return new MemberDescription(member.memberId(),
                                     Optional.ofNullable(member.groupInstanceId()),
                                     member.clientId(),
                                     member.clientHost(),
                                     assignment);
    }

    @SafeVarargs
    private static <T> void assertCollectionIs(Collection<T> collection, T... elements) {
        for (T element : elements) {
            assertTrue("Did not find " + element, collection.contains(element));
        }
        assertEquals("There are unexpected extra elements in the collection.",
            elements.length, collection.size());
    }

    public static KafkaAdminClient createInternal(AdminClientConfig config, KafkaAdminClient.TimeoutProcessorFactory timeoutProcessorFactory) {
        return KafkaAdminClient.createInternal(config, timeoutProcessorFactory);
    }

    public static class FailureInjectingTimeoutProcessorFactory extends KafkaAdminClient.TimeoutProcessorFactory {

        private int numTries = 0;

        private int failuresInjected = 0;

        @Override
        public KafkaAdminClient.TimeoutProcessor create(long now) {
            return new FailureInjectingTimeoutProcessor(now);
        }

        synchronized boolean shouldInjectFailure() {
            numTries++;
            if (numTries == 1) {
                failuresInjected++;
                return true;
            }
            return false;
        }

        public synchronized int failuresInjected() {
            return failuresInjected;
        }

        public final class FailureInjectingTimeoutProcessor extends KafkaAdminClient.TimeoutProcessor {
            public FailureInjectingTimeoutProcessor(long now) {
                super(now);
            }

            boolean callHasExpired(KafkaAdminClient.Call call) {
                if ((!call.isInternal()) && shouldInjectFailure()) {
                    log.debug("Injecting timeout for {}.", call);
                    return true;
                } else {
                    boolean ret = super.callHasExpired(call);
                    log.debug("callHasExpired({}) = {}", call, ret);
                    return ret;
                }
            }
        }
    }
}
