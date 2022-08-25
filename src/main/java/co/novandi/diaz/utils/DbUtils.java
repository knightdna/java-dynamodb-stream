package co.novandi.diaz.utils;

import lombok.experimental.UtilityClass;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;

import java.net.URI;

import static co.novandi.diaz.config.DbConfig.*;

@UtilityClass
public class DbUtils {

    public DynamoDbAsyncClient createAsyncClient() {
        return DynamoDbAsyncClient.builder()
                .credentialsProvider(() -> AwsBasicCredentials.create(ACCESS_KEY_ID, SECRET_ACCESS_KEY))
                .endpointOverride(URI.create(DYNAMODB_URL))
                .endpointDiscoveryEnabled(false)
                .region(REGION)
                .build();
    }

    public static DynamoDbStreamsAsyncClient createStreamsAsyncClient() {
        return DynamoDbStreamsAsyncClient.builder()
                .credentialsProvider(() -> AwsBasicCredentials.create(ACCESS_KEY_ID, SECRET_ACCESS_KEY))
                .endpointOverride(URI.create(DYNAMODB_URL))
                .region(REGION)
                .build();
    }

}
