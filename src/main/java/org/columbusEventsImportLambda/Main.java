package org.columbusEventsImportLambda;

import org.columbusEventsImportLambda.service.aws.DynamoDBReader;
import org.columbusEventsImportLambda.service.aws.DynamoDbClientFactory;
import org.columbusEventsImportLambda.service.ImportService;
import org.columbusEventsImportLambda.service.google.GoogleSheetService;
import org.columbusEventsImportLambda.models.DynamoDBEvent;
import org.columbusEventsImportLambda.models.GoogleEvent;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.List;

import static org.columbusEventsImportLambda.service.ImportService.GOOGLE_SHEET_ID;

public class Main {

    public static void main(String[] args) throws Exception {
        final String sheetId = System.getenv("COLUMBUS_GOOGLE_SHEET_ID");
        final String range = "A2:D";

        GoogleSheetService googleSheetService = new GoogleSheetService();
        DynamoDBReader dynamoDBReader = new DynamoDBReader("airbyte_sync_ColumbusEvents");
        ImportService importService = new ImportService(
                DynamoDbClient.create(),
                new GoogleSheetService(),
                System.getenv(GOOGLE_SHEET_ID)
        );

        List<GoogleEvent> googleEvents = googleSheetService.fetchEvents(sheetId, range);
        List<DynamoDBEvent> dynamoDBEvents = dynamoDBReader.fetchEvents(DynamoDbClientFactory.getClient());

        importService.importGoogleRecordsToDynamoDB(googleEvents, dynamoDBEvents);

    }
}

