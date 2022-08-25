package co.novandi.diaz.client;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;

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
        var describeTableRequest = DescribeTableRequest.builder().tableName(TABLE_NAME).build();
        var describeTableAsyncResult = asyncDbClient.describeTable(describeTableRequest);

        describeTableAsyncResult.whenComplete((describeTableResponse, describeTableError) -> {
            if (describeTableResponse != null) {
                var tableDescription = describeTableResponse.table();
                var streamSpecification = tableDescription.streamSpecification();
                log.info("Stream view type: " + streamSpecification.streamViewType());

                String streamArn = tableDescription.latestStreamArn();
                log.info("Stream ARN: " + streamArn);

                var describeStreamRequest = DescribeStreamRequest.builder().streamArn(streamArn).build();
                var describeStreamsAsyncResult = asyncStreamsClient.describeStream(describeStreamRequest);

                describeStreamsAsyncResult.whenComplete((describeStreamsResponse, describeStreamsError) -> {
                    if (describeStreamsResponse != null) {
                        describeStreamsResponse.streamDescription().shards().forEach(shard -> {
                            String shardId = shard.shardId();
                            log.info("Shard ID: " + shardId);

                            var getShardIteratorRequest = GetShardIteratorRequest.builder()
                                    .streamArn(streamArn)
                                    .shardId(shardId)
                                    .shardIteratorType(ShardIteratorType.LATEST)
                                    .build();
                            var getShardIteratorAsyncResult = asyncStreamsClient.getShardIterator(getShardIteratorRequest);

                            getShardIteratorAsyncResult.whenComplete((getShardIteratorResponse, getShardIteratorError) -> {
                                if (getShardIteratorResponse != null) {
                                    String currentShardIterator = getShardIteratorResponse.shardIterator();
                                    while (currentShardIterator != null) {
                                        var getRecordsRequest = GetRecordsRequest.builder().shardIterator(currentShardIterator).limit(100).build();
                                        var getRecordsAsyncResult = asyncStreamsClient.getRecords(getRecordsRequest);

                                        try {
                                            GetRecordsResponse getRecordsResponse = getRecordsAsyncResult.get(10L, TimeUnit.SECONDS);
                                            getRecordsResponse.records().forEach(record -> log.info("RECORD: " + record.dynamodb()));
                                            currentShardIterator = getRecordsResponse.nextShardIterator();
                                        } catch (Exception e) {
                                            log.error("Unable to get the value of the getRecordsAsyncResult. Reason: " + e.getMessage());
                                        }
                                        getRecordsAsyncResult.join();
                                    }
                                } else {
                                    log.error("Unable to get the value of the getShardIteratorAsyncResult. Reason: " + getShardIteratorError.getMessage());
                                }
                            });
                            getShardIteratorAsyncResult.join();
                        });
                    } else {
                        log.error("Unable to get the value of the describeStreamsAsyncResult. Reason: " + describeStreamsError.getMessage());
                    }
                });
                describeStreamsAsyncResult.join();
            } else {
                log.info("Unable to get the value of the describeTableAsyncResult. Reason: " + describeTableError.getMessage());
            }
        });
        describeTableAsyncResult.join();
    }

}
