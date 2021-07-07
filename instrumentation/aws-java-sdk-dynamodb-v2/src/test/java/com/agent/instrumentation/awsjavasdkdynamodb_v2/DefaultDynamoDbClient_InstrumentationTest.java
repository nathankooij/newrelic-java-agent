/*
 *
 *  * Copyright 2020 New Relic Corporation. All rights reserved.
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package com.agent.instrumentation.awsjavasdkdynamodb_v2;

import com.newrelic.agent.bridge.datastore.DatastoreVendor;
import com.newrelic.agent.introspec.DatastoreHelper;
import com.newrelic.agent.introspec.InstrumentationTestConfig;
import com.newrelic.agent.introspec.InstrumentationTestRunner;
import com.newrelic.agent.introspec.Introspector;
import com.newrelic.api.agent.Trace;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static junit.framework.TestCase.assertEquals;

@RunWith(InstrumentationTestRunner.class)
@InstrumentationTestConfig(includePrefixes = { "software.amazon.awssdk.services.dynamodb", "com.nr.instrumentation" })
public class DefaultDynamoDbClient_InstrumentationTest {

    private static final String DYNAMODB_PRODUCT = DatastoreVendor.DynamoDB.toString();
    private static final String TABLE_NAME = "test";
    private static final String SECOND_TABLE_NAME = "second_table";

    private static LocalTestDynamoDb dynamoDb;
    private static String hostName;
    private static String port;

    @BeforeClass
    public static void beforeClass() throws Exception {
        dynamoDb = LocalTestDynamoDb.create();
        port = dynamoDb.getPort();
        hostName = dynamoDb.getHostName();
        dynamoDb.start();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        dynamoDb.stop();
    }

    @Before
    public void beforeEach() {
        // start with no table for each test
        if (dynamoDb.tableExists()) {
            dynamoDb.deleteTable();
        }
    }

    @Test
    public void testCreateTable() {
        // when
        trace(dynamoDb::createTable);
        // then
        assertTableOperation("createTable");
    }

    @Test
    public void testCreateTableAsync() {
        // when
        trace(dynamoDb::createTableAsync);
        // then
        assertTableOperation("createTable");
    }

    @Test
    public void testPutGetUpdateDeleteItem() {
        // given
        dynamoDb.createTable();
        // when
        trace(new Runnable[] {
                dynamoDb::putItem,
                dynamoDb::getItem,
                dynamoDb::updateItem,
                dynamoDb::deleteItem
        });
        // then
        assertTableOperations(new String[] { "putItem", "getItem", "updateItem", "deleteItem" });
    }

    @Test
    public void testPutGetUpdateDeleteItemAsync() {
        // given
        dynamoDb.createTable();
        // when
        trace(new Runnable[] {
                dynamoDb::putItemAsync,
                dynamoDb::getItemAsync,
                dynamoDb::updateItemAsync,
                dynamoDb::deleteItemAsync
        });
        // then
        assertTableOperations(new String[] { "putItem", "getItem", "updateItem", "deleteItem" });
    }

    @Test
    public void testQuery() {
        // given
        dynamoDb.createTable();
        // when
        trace(dynamoDb::query);
        // then
        assertTableOperation("query");
    }

    @Test
    public void testQueryAsync() {
        // given
        dynamoDb.createTable();
        // when
        trace(dynamoDb::queryAsync);
        // then
        assertTableOperation("query");
    }

    @Test
    public void testListTables() {
        // given
        dynamoDb.createTable();
        // when
        trace(dynamoDb::listTables);
        // then
        assertOperation("listTables");
    }

    @Test
    public void testDescribeTable() {
        // given
        dynamoDb.createTable();
        // when
        trace(dynamoDb::describeTable);
        // then
        assertTableOperation("describeTable");
    }

    @Test
    public void testDescribeTableAsync() {
        // given
        dynamoDb.createTable();
        // when
        trace(dynamoDb::describeTableAsync);
        // then
        assertTableOperation("describeTable");
    }

    @Test
    public void testScan() {
        // given
        dynamoDb.createTable();
        // when
        trace(dynamoDb::scan);
        // then
        assertTableOperation("scan");
    }

    @Test
    public void testScanAsync() {
        // given
        dynamoDb.createTable();
        // when
        trace(dynamoDb::scanAsync);
        // then
        assertTableOperation("scan");
    }

    @Test
    public void testDeleteTable() {
        // given
        dynamoDb.createTable();
        // when
        trace(dynamoDb::deleteTable);
        // then
        assertTableOperation("deleteTable");
    }

    @Trace(dispatcher = true)
    private void trace(Runnable runnable) {
        runnable.run();
    }

    @Trace(dispatcher = true)
    private void trace(Runnable[] actions) {
        Arrays.stream(actions).forEach(Runnable::run);
    }

    private void assertOperation(String operation) {
        Introspector introspector = InstrumentationTestRunner.getIntrospector();
        assertEquals(1, introspector.getFinishedTransactionCount(10000));

        String txName = introspector.getTransactionNames().iterator().next();
        DatastoreHelper helper = new DatastoreHelper(DYNAMODB_PRODUCT);
        helper.assertAggregateMetrics();
        helper.assertScopedOperationMetricCount(txName, operation, 1);
        helper.assertInstanceLevelMetric(DYNAMODB_PRODUCT, hostName, port);
    }

    private void assertTableOperation(String operation) {
        Introspector introspector = InstrumentationTestRunner.getIntrospector();
        assertEquals(1, introspector.getFinishedTransactionCount(10000));

        String txName = introspector.getTransactionNames().iterator().next();
        DatastoreHelper helper = new DatastoreHelper(DYNAMODB_PRODUCT);
        helper.assertAggregateMetrics();
        helper.assertScopedStatementMetricCount(txName, operation, TABLE_NAME, 1);
        helper.assertInstanceLevelMetric(DYNAMODB_PRODUCT, hostName, port);
    }

    private void assertTableOperations(String[] operations) {
        Introspector introspector = InstrumentationTestRunner.getIntrospector();
        assertEquals(1, introspector.getFinishedTransactionCount(10000));

        String txName = introspector.getTransactionNames().iterator().next();
        DatastoreHelper helper = new DatastoreHelper(DYNAMODB_PRODUCT);
        helper.assertAggregateMetrics();
        Arrays.stream(operations)
                .forEach(operation -> helper.assertScopedStatementMetricCount(txName, operation, TABLE_NAME, 1));

        helper.assertInstanceLevelMetric(DYNAMODB_PRODUCT, hostName, port);
    }
}
