package org.columbusEventsSyncLambda;




public class Main {

    public static void main(String[] args) throws Exception {

        String sheet = GoogleSheetReader.fetchSheetData();
        System.out.println(sheet);
    }
}


//    private static void addRecordsToDatabase(CSVParser records) {
//        DynamoDbClient dynamoDb = DynamoDbClient.create();
//        for (CSVRecord record : records) {
//            Map<String, AttributeValue> item = new HashMap<>();
///*
//* Iterates through each column of the record and gets the key(header)/value EX: "locationName" -> "Nationwide Arena"
//* adds to the map
//* item example: "locationName -> {AttributeValue@4286} "AttributeValue(S=Convention Center)""
//* */
//            for (String header : records.getHeaderNames()) {
//                String value = record.get(header);
//                if (value != null) {
//                    item.put(header, AttributeValue.builder().s(value).build());
//                }
//            }
//
//            PutItemRequest request = PutItemRequest.builder()
//                    .tableName(TABLE_NAME)
//                    .item(item)
//                    .build();
////TODO put all items at the same time
//            dynamoDb.putItem(request);
//            System.out.println("Inserted item: " + item);
//        }
//    }
