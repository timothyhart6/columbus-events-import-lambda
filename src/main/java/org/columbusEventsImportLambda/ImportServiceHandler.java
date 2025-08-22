package org.columbusEventsImportLambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class ImportServiceHandler implements RequestHandler<Void, Void> {

    @Override
    public Void handleRequest(Void input, Context context) {
        try {
            new ImportJobRunner().run();
        } catch (Exception e) {
            throw new RuntimeException("Import job failed", e);
        }
        return null;
    }
}

