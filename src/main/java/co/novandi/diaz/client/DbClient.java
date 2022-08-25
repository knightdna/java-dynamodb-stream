package co.novandi.diaz.client;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbAsyncWaiter;

import java.util.Map;

@RequiredArgsConstructor
@Slf4j
public class DbClient {

    @NonNull
    private DynamoDbAsyncClient asyncDbClient;

    public void createTable(String tableName, String key) {
        createTable(tableName, key, StreamViewType.KEYS_ONLY);
    }

    public void createTable(String tableName, String key, StreamViewType streamViewType) {
        final DynamoDbAsyncClient client = this.asyncDbClient;

        // Create a DynamoDbAsyncWaiter object
        DynamoDbAsyncWaiter asyncWaiter = client.waiter();

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
                        .readCapacityUnits(10L)
                        .writeCapacityUnits(10L)
                        .build())
                .streamSpecification(streamSpecification)
                .build();

        // Create the table by using the DynamoDbAsyncClient object
        var createTableAsyncResult = client.createTable(createTableRequest);
        createTableAsyncResult.whenComplete((createTableResponse, error) -> {
            // Let the application shut down. Only close the client when you are completely done with it.
            try (client) {
                if (createTableResponse != null) {
                    // Create a DescribeTableRequest object required for waiter functionality
                    var describeTableRequest = DescribeTableRequest.builder()
                            .tableName(createTableResponse.tableDescription().tableName())
                            .build();

                    var describeTableAsyncResult = asyncWaiter.waitUntilTableExists(describeTableRequest);

                    // Fires when the table is ready
                    describeTableAsyncResult.whenComplete((describeTableResponse, throwable) -> {
                        // Print out the new table's ARN when it's ready
                        describeTableResponse.matched().response().ifPresent(response -> {
                            String tableArn = response.table().tableArn();
                            log.info("The table {} is ready", tableArn);
                        });
                    });
                    describeTableAsyncResult.join();
                } else {
                    // Handle error
                    log.error("Unable to get value of the createTableAsyncResult. Reason: " + error.getMessage());
                }
            }
        });
        createTableAsyncResult.join();
    }

    public void insertItem(String tableName, Map<String, AttributeValue> item) {
        var putItemRequest = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build();

        var putItemAsyncResult = asyncDbClient.putItem(putItemRequest);
        putItemAsyncResult.whenComplete((putItemResponse, putItemError) -> {
            if (putItemResponse != null) {
                putItemResponse.attributes().forEach((key, value) -> log.info("Key: {}, value: {}", key, value));
            } else {
                log.error("Unable to get value of the putItemAsyncResult. Reason: " + putItemError.getMessage());
            }
        });
        putItemAsyncResult.join();
    }

}
