package org.columbusEventsImportLambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.util.Map;

public class ImportServiceHandler implements RequestHandler<Map<String, Object>, Void> {

    @Override
    public Void handleRequest(Map<String, Object> input, Context context) {
        try {
            new ImportJobRunner().run();
        } catch (Exception e) {
            throw new RuntimeException("Import job failed", e);
        }
        return null; // no output
    }
}
