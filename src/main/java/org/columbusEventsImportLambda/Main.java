package org.columbusEventsImportLambda;

import org.columbusEventsImportLambda.aws.DynamoDBReader;
import org.columbusEventsImportLambda.aws.DynamoDbClientFactory;
import org.columbusEventsImportLambda.google.GoogleSheetService;
import org.columbusEventsImportLambda.models.DynamoDBEvent;
import org.columbusEventsImportLambda.models.GoogleEvent;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.List;

public class Main {

    public static void main(String[] args) throws Exception {
        final String sheetId = System.getenv("COLUMBUS_GOOGLE_SHEET_ID");
        final String range = "A2:D";

        GoogleSheetService googleSheetService = new GoogleSheetService();
        DynamoDBReader dynamoDBReader = new DynamoDBReader("airbyte_sync_ColumbusEvents");
        Importer importer = new Importer(DynamoDbClient.create());

        List<GoogleEvent> googleEvents = googleSheetService.fetchEvents(sheetId, range);
        List<DynamoDBEvent> dynamoDBEvents = dynamoDBReader.fetchEvents(DynamoDbClientFactory.getClient());

        importer.importGoogleRecordsToDynamoDB(googleEvents, dynamoDBEvents);

    }
}

