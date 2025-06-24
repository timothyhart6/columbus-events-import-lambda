package org.columbusEventsSyncLambda;

import org.columbusEventsSyncLambda.models.Event;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.List;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws Exception {
        final String sheetId = System.getenv("COLUMBUS_GOOGLE_SHEET_ID");
        final String range = "A2:D";

        GoogleSheetReader googleSheetReader = new GoogleSheetReader();
        DynamoDBReader dynamoDBReader = new DynamoDBReader("airbyte_sync_ColumbusEvents");
        List<Event> googleEvents = googleSheetReader.fetchEvents(sheetId, range);

        List<Event> dynamoDBEvents = dynamoDBReader.getAllEvents(DynamoDbClientFactory.getClient());



        //        System.out.println(googleEvents);
//        System.out.println(dynamoDBEvents);
    }
}

