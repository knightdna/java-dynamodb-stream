package co.novandi.diaz.client;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.Map;
import java.util.Optional;

import static co.novandi.diaz.config.DbConfig.DEFAULT_READ_CAPACITY_UNITS;
import static co.novandi.diaz.config.DbConfig.DEFAULT_WRITE_CAPACITY_UNITS;

@RequiredArgsConstructor
@Slf4j
public class DbClient {

    @NonNull
    private DynamoDbAsyncClient asyncDbClient;

    public void createTable(String tableName, String key) {
        createTable(tableName, key, StreamViewType.KEYS_ONLY);
    }

    public void createTable(String tableName, String key, StreamViewType streamViewType) {
        StreamSpecification streamSpecification = StreamSpecification.builder()
                .streamEnabled(true)
                .streamViewType(streamViewType)
                .build();

        // Create the CreateTableRequest object
        CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .tableName(tableName)
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName(key)
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .keySchema(KeySchemaElement.builder()
                        .attributeName(key)
                        .keyType(KeyType.HASH)
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(DEFAULT_READ_CAPACITY_UNITS)
                        .writeCapacityUnits(DEFAULT_WRITE_CAPACITY_UNITS)
                        .build())
                .streamSpecification(streamSpecification)
                .build();

        final DynamoDbAsyncClient client = this.asyncDbClient;
        client.createTable(createTableRequest)
                .whenComplete((createTableResponse, error) -> {
                    // Let the application shut down. Only close the client when you are completely done with it.
                    try (client) {
                        Optional.ofNullable(createTableResponse).ifPresentOrElse(
                                response -> client.waiter().waitUntilTableExists(DescribeTableRequest.builder()
                                                .tableName(response.tableDescription().tableName())
                                                .build())
                                        .whenComplete((waitUntilTableExistsResponse, waitUntilTableExistsError) ->
                                                waitUntilTableExistsResponse.matched().response()
                                                        .ifPresent(describeTableResponse -> log.info("The table {} is ready", describeTableResponse.table().tableArn())))
                                        .join(),
                                () -> log.error("Unable to create table {}. Reason: {}", tableName, error.getMessage()));
                    }
                })
                .join();
    }

    public void insertItem(String tableName, Map<String, AttributeValue> item) {
        asyncDbClient.putItem(PutItemRequest.builder()
                        .tableName(tableName)
                        .item(item)
                        .build())
                .whenComplete((putItemResponse, putItemError) ->
                        Optional.ofNullable(putItemResponse).ifPresentOrElse(
                                response -> response.attributes().forEach((key, value) -> log.info("Key: {}, value: {}", key, value)),
                                () -> log.error("Unable to put the item {} on the table {}. Reason: {}", item, tableName, putItemError.getMessage())
                        ))
                .join();
    }

}
