package org.columbusEventsImportLambda.aws;

import lombok.Getter;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDbClientFactory {

    @Getter
    private static final DynamoDbClient client = initClient();

    private static DynamoDbClient initClient() {
        return DynamoDbClient.builder()
                .credentialsProvider(isRunningLocally()
                        ? ProfileCredentialsProvider.create("local")
                        : DefaultCredentialsProvider.create())
                .region(Region.US_EAST_1)
                .build();
    }

    private static boolean isRunningLocally() {
        return System.getenv("AWS_EXECUTION_ENV") == null;
        // AWS Lambda sets this environment variable, so if it's null, we're running locally
    }
}