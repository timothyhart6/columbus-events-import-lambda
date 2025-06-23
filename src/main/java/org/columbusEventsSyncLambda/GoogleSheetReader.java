package org.columbusEventsSyncLambda;

import lombok.extern.slf4j.Slf4j;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.api.client.http.javanet.NetHttpTransport;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

@Slf4j
public class GoogleSheetReader {

    HttpRequestFactory requestFactory;

    public GoogleSheetReader() {
        this.requestFactory = getHttpRequestFactory();
    }

//  Used for testing purposes
    public GoogleSheetReader(HttpRequestFactory requestFactory) {
        this.requestFactory = requestFactory;
    }

    public String fetchSheetData(String sheetId, String range) {
        try {
            if (requestFactory == null) {
                log.warn("HttpRequestFactory is not initialized.");
                return "";
            }
            String url = String.format(
                   "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s",
                   sheetId, range);
            GenericUrl genericUrl = new GenericUrl(url);
            HttpResponse response = requestFactory.buildGetRequest(genericUrl).execute();
            return response.parseAsString();
        } catch (IOException e) {
            log.warn("No google sheet data available: {}", e.getMessage());
            return "";
        }
    }

    HttpRequestFactory getHttpRequestFactory() {
        try {
            HttpRequestFactory requestFactory = null;
            HttpTransport transport = new NetHttpTransport();
            GoogleCredentials credentials = loadGoogleCredentials();
            if (credentials != null) {
                requestFactory = transport.createRequestFactory(new HttpCredentialsAdapter(credentials));
            }
            if (requestFactory == null) {
                log.warn("HttpRequestFactory is null; cannot fetch sheet data.");
                return null;
            }
            return requestFactory;

        } catch (IOException | IllegalStateException e) {
            log.warn("Cannot retrieve google credentials: {}", e.getMessage());
            return null;
        }
    }

    GoogleCredentials loadGoogleCredentials() throws IOException {
        String json = System.getenv("GCP_CREDENTIALS_JSON");
        if (json == null || json.isEmpty()) {
            throw new IllegalStateException("Missing GCP_CREDENTIALS_JSON environment variable");
        }
        return GoogleCredentials.fromStream(
            new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))
        ).createScoped(Collections.singleton("https://www.googleapis.com/auth/spreadsheets.readonly"));
    }

}