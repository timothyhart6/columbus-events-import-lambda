package org.columbusEventsImportLambda;

import org.columbusEventsImportLambda.aws.DynamoDBReader;
import org.columbusEventsImportLambda.aws.DynamoDbClientFactory;
import org.columbusEventsImportLambda.google.GoogleSheetReader;
import org.columbusEventsImportLambda.models.DynamoDBEvent;
import org.columbusEventsImportLambda.models.GoogleEvent;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.List;

public class Main {

    public static void main(String[] args) throws Exception {
        final String sheetId = System.getenv("COLUMBUS_GOOGLE_SHEET_ID");
        final String range = "A2:D";

        GoogleSheetReader googleSheetReader = new GoogleSheetReader();
        DynamoDBReader dynamoDBReader = new DynamoDBReader("airbyte_sync_ColumbusEvents");
        Importer importer = new Importer(DynamoDbClient.create());
        List<GoogleEvent> googleEvents = googleSheetReader.fetchEvents(sheetId, range);
        List<DynamoDBEvent> dynamoDBEvents = dynamoDBReader.getAllEvents(DynamoDbClientFactory.getClient());

        importer.importGoogleRecordsToDynamoDB(googleEvents, dynamoDBEvents);

    }
}

