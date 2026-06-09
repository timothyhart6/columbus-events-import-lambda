package org.columbusEventsImportLambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ImportServiceHandler implements RequestHandler<Void, Void> {

    @Override
    public Void handleRequest(Void input, Context context) {
        try {
            log.info("Importing events from Google Sheet to DynamoDB");
            new ImportJobRunner().run();
            log.info("Imported events from Google Sheet to DynamoDB");
        } catch (Exception e) {
            throw new RuntimeException("Import job failed", e);
        }

        return null;
    }
}
