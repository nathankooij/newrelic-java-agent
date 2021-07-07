package com.agent.instrumentation.awsjavasdkdynamodb_v2;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.newrelic.agent.introspec.InstrumentationTestRunner;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.net.InetAddress;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class LocalTestDynamoDb {
    private static final String TABLE_NAME = "test";

    private final String hostName;
    private final String port;
    private final DynamoDBProxyServer server;
    private final DynamoDbClient client;
    private final DynamoDbAsyncClient asyncClient;

    private LocalTestDynamoDb() throws Exception {
        port = String.valueOf(InstrumentationTestRunner.getIntrospector().getRandomPort());
        hostName = InetAddress.getLocalHost().getHostName();
        server = ServerRunner.createServerFromCommandLineArgs(new String[] { "-inMemory", "-port", port });
        client = DynamoDbClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.builder().build())
                .endpointOverride(new URI("http://localhost:" + port))
                .region(Region.US_WEST_1).build();
        asyncClient = DynamoDbAsyncClient.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .endpointOverride(new URI("http://localhost:" + port))
                .region(Region.US_WEST_1).build();
    }

    public static LocalTestDynamoDb create() throws Exception {
        return new LocalTestDynamoDb();
    }

    private static void tryToGetCompletableFuture(CompletableFuture<?> completableFuture) {
        try {
            completableFuture.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getHostName() {
        return hostName;
    }

    public String getPort() {
        return port;
    }

    public void start() throws Exception {
        server.start();
    }

    public void stop() throws Exception {
        server.stop();
    }

    public void getItem() {
        client.getItem(getItemRequest());
    }

    public void getItemAsync() {
        tryToGetCompletableFuture(asyncClient.getItem(getItemRequest()));
    }

    private GetItemRequest getItemRequest() {
        return GetItemRequest.builder()
                .tableName(TABLE_NAME)
                .key(Collections.singletonMap("artist", AttributeValue.builder().s("Pink").build()))
                .build();
    }

    public void putItem() {
        client.putItem(putItemRequest());
    }

    public void putItemAsync() {
        tryToGetCompletableFuture(asyncClient.putItem(putItemRequest()));
    }

    private PutItemRequest putItemRequest() {
        return PutItemRequest.builder()
                .tableName(TABLE_NAME)
                .item(createDefaultItem())
                .build();
    }

    public void updateItem() {
        client.updateItem(updateItemRequest());
    }

    public void updateItemAsync() {
        tryToGetCompletableFuture(asyncClient.updateItem(updateItemRequest()));
    }

    private UpdateItemRequest updateItemRequest() {
        Map<String, AttributeValue> itemKey = new HashMap<>();

        itemKey.put("artist", AttributeValue.builder().s("Pink").build());

        Map<String, AttributeValueUpdate> updatedValues = new HashMap<>();

        updatedValues.put("rating", AttributeValueUpdate.builder()
                .value(AttributeValue.builder().s("5 stars").build())
                .action(AttributeAction.PUT)
                .build());

        return UpdateItemRequest.builder()
                .tableName(TABLE_NAME)
                .key(itemKey)
                .attributeUpdates(updatedValues)
                .build();
    }

    public void deleteItem() {
        client.deleteItem(deleteItemRequest());
    }

    public void deleteItemAsync() {
        tryToGetCompletableFuture(asyncClient.deleteItem(deleteItemRequest()));
    }

    private DeleteItemRequest deleteItemRequest() {
        return DeleteItemRequest.builder()
                .tableName(TABLE_NAME)
                .key(Collections.singletonMap("artist", AttributeValue.builder().s("Pink").build()))
                .build();
    }

    public Map<String, AttributeValue> createDefaultItem() {
        Map<String, AttributeValue> itemValues = new HashMap<>();

        // Add all content to the table
        itemValues.put("artist", AttributeValue.builder().s("Pink").build());
        itemValues.put("songTitle", AttributeValue.builder().s("lazy river").build());
        return itemValues;
    }

    public void listTables() {
        client.listTables(ListTablesRequest.builder().build());
    }

    public void query() {
        client.query(queryRequest());
    }

    public void queryAsync() {
        tryToGetCompletableFuture(asyncClient.query(queryRequest()));
    }

    private QueryRequest queryRequest() {
        Map<String, String> attrNameAlias = new HashMap<>();
        attrNameAlias.put("#artist", "artist");

        Map<String, AttributeValue> attrValues =
                new HashMap<>();

        attrValues.put(":artist", AttributeValue.builder()
                .s("Miles Davis")
                .build());

        return QueryRequest.builder()
                .tableName(TABLE_NAME)
                .keyConditionExpression("#artist = :artist")
                .expressionAttributeNames(attrNameAlias)
                .expressionAttributeValues(attrValues)
                .build();
    }

    public void scan() {
        client.scan(ScanRequest.builder().tableName(TABLE_NAME).build());
    }

    public void scanAsync() {
        tryToGetCompletableFuture(asyncClient.scan(ScanRequest.builder().tableName(TABLE_NAME).build()));
    }

    public boolean tableExists() {
        ListTablesRequest request = ListTablesRequest.builder().build();
        ListTablesResponse listTableResponse = client.listTables(request);
        return listTableResponse.tableNames().contains(TABLE_NAME);
    }

    public void deleteTable() {
        client.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());

    }

    public void describeTable() {
        client.describeTable(DescribeTableRequest.builder().tableName(TABLE_NAME).build());
    }

    public void describeTableAsync() {
        tryToGetCompletableFuture(
                asyncClient.describeTable(DescribeTableRequest.builder().tableName(TABLE_NAME).build()));
    }

    private CreateTableRequest createTableRequest() {
        return CreateTableRequest.builder()
                .tableName(TABLE_NAME)
                .keySchema(KeySchemaElement.builder().attributeName("artist").keyType(KeyType.HASH).build())
                .attributeDefinitions(AttributeDefinition.builder().attributeName("artist").attributeType(ScalarAttributeType.S).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build();
    }

    public void createTable() {
        client.createTable(createTableRequest());
    }

    public void createTableAsync() {
        tryToGetCompletableFuture(asyncClient.createTable(createTableRequest()));
    }
}
