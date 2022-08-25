package co.novandi.diaz;

import co.novandi.diaz.client.DbClient;
import co.novandi.diaz.client.DbStreamListener;
import co.novandi.diaz.config.DbConfig;
import co.novandi.diaz.utils.DbUtils;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class App {

    public static void main(String[] args) {
        listenToStream();
    }

    public static void listenToStream() {
        DbStreamListener listener = new DbStreamListener(DbUtils.createAsyncClient(), DbUtils.createStreamsAsyncClient());

        log.info("Start listening to stream");
        listener.listen();
    }

    public static void createTable() {
        DbClient client = new DbClient(DbUtils.createAsyncClient());

        log.info("Creating table");
        client.createTable("sampleStreamedTableKeysOnly", "sampleKey", StreamViewType.KEYS_ONLY);
    }

    public static void insertItem() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("sampleKey", AttributeValue.builder().s("789").build());
        item.put("data", AttributeValue.builder()
                .m(Map.of("publishingSite", AttributeValue.builder().s("dplay.no").build()))
                .build());

        DbClient client = new DbClient(DbUtils.createAsyncClient());

        log.info("Inserting an item");
        client.insertItem(DbConfig.TABLE_NAME, item);
    }

}
