package co.novandi.diaz.client;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static co.novandi.diaz.config.DbConfig.TABLE_NAME;

@RequiredArgsConstructor
@Slf4j
public class DbStreamListener {

    @NonNull
    private DynamoDbAsyncClient asyncDbClient;
    @NonNull
    private DynamoDbStreamsAsyncClient asyncStreamsClient;

    public void listen() {
        asyncDbClient.describeTable(DescribeTableRequest.builder()
                        .tableName(TABLE_NAME)
                        .build())
                .whenComplete((describeTableResponse, describeTableError) ->
                        Optional.ofNullable(describeTableResponse).ifPresentOrElse(
                                this::observeTable,
                                () -> log.info("Unable to get the value of the describeTableAsyncResult. Reason: " + describeTableError.getMessage())))
                .join();
    }

    private void observeTable(DescribeTableResponse describeTableResponse) {
        this.observeTableStream(describeTableResponse.table().latestStreamArn());
    }

    private void observeTableStream(String tableStreamArn) {
        log.info("Observing table stream ARN: {}", tableStreamArn);

        asyncStreamsClient.describeStream(DescribeStreamRequest.builder()
                        .streamArn(tableStreamArn)
                        .build())
                .whenComplete((describeStreamResponse, describeStreamsError) ->
                        Optional.ofNullable(describeStreamResponse).ifPresentOrElse(
                                response -> response.streamDescription().shards().forEach(shard -> this.observeShard(shard, tableStreamArn)),
                                () -> log.error("Unable to observe the table stream {}. Reason: {}", tableStreamArn, describeStreamsError.getMessage())))
                .join();
    }

    private void observeShard(Shard shard, String tableStreamArn) {
        String shardId = shard.shardId();
        log.info("Observing shard ID: {}", shardId);

        asyncStreamsClient.getShardIterator(GetShardIteratorRequest.builder()
                        .streamArn(tableStreamArn)
                        .shardId(shardId)
                        .shardIteratorType(ShardIteratorType.LATEST)
                        .build())
                .whenComplete((getShardIteratorResponse, getShardIteratorError) ->
                        Optional.ofNullable(getShardIteratorResponse).ifPresentOrElse(
                                this::listenToRecordChanges,
                                () -> log.error("Unable to iterate on shard: {}. Reason: {}", shardId, getShardIteratorError.getMessage())))
                .join();
    }

    private void listenToRecordChanges(GetShardIteratorResponse getShardIteratorResponse) {
        final long executionTimeoutInSeconds = 10L;
        final int maxNumberOfReturnedRecords = 100;

        String currentShardIterator = getShardIteratorResponse.shardIterator();
        while (currentShardIterator != null) {
            var getRecordsAsyncResult = asyncStreamsClient.getRecords(GetRecordsRequest.builder()
                    .shardIterator(currentShardIterator)
                    .limit(maxNumberOfReturnedRecords)
                    .build());
            try {
                GetRecordsResponse getRecordsResponse = getRecordsAsyncResult.get(executionTimeoutInSeconds, TimeUnit.SECONDS);
                getRecordsResponse.records().forEach(record -> log.info("RECORD: " + record.dynamodb()));
                currentShardIterator = getRecordsResponse.nextShardIterator();
            } catch (Exception e) {
                log.error("Unable to listen to record changes. Reason: " + e.getMessage());
            }
            getRecordsAsyncResult.join();
        }
    }

}
