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

    public String fetchSheetData(String sheetId, String range) throws IOException {
    String stringResponse = "";
        try {

            String url = String.format(
                   "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s",
                   sheetId, range);
            GenericUrl genericUrl = new GenericUrl(url);
            HttpResponse response = requestFactory.buildGetRequest(genericUrl).execute();
            stringResponse = response.parseAsString();
        } catch (NullPointerException | IOException e) {
            log.info("No google sheet data available: {}", e.getMessage());
        }
        return stringResponse;
    }

    HttpRequestFactory getHttpRequestFactory() {
        HttpRequestFactory requestFactory = null;
        try {
            HttpTransport transport = new NetHttpTransport();
            GoogleCredentials credentials = loadGoogleCredentials();
            requestFactory = transport.createRequestFactory(new HttpCredentialsAdapter(credentials));
        } catch (IllegalStateException | IOException e) {
            log.info(e.getMessage());
        }
        return requestFactory;
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
