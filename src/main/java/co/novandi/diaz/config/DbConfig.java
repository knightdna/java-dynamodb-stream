package co.novandi.diaz.config;

import software.amazon.awssdk.regions.Region;

public class DbConfig {

    public static final String ACCESS_KEY_ID = "local";
    public static final String SECRET_ACCESS_KEY = "local";

    public static final String DYNAMODB_URL = "http://localhost:8000";
    public static final Region REGION = Region.US_WEST_2;

    public static final String TABLE_NAME = "sampleStreamedTable";

}
