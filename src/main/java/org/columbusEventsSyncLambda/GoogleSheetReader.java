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

    private static final String SHEET_ID = System.getenv("COLUMBUS_GOOGLE_SHEET_ID");
    private static final String RANGE = "A2:D";

    public static String fetchSheetData() throws IOException {
        GoogleCredentials credentials = loadGoogleCredentials();
        HttpTransport transport = new NetHttpTransport();
        HttpRequestFactory requestFactory = transport.createRequestFactory(new HttpCredentialsAdapter(credentials));

        String url = String.format(
                "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s",
                SHEET_ID, RANGE
        );

        GenericUrl genericUrl = new GenericUrl(url);
        HttpResponse response = requestFactory.buildGetRequest(genericUrl).execute();
        return response.parseAsString();
    }

    private static GoogleCredentials loadGoogleCredentials() throws IOException {
        String json = System.getenv("GCP_CREDENTIALS_JSON");
        if (json == null || json.isEmpty()) {
            throw new IllegalStateException("Missing GCP_CREDENTIALS_JSON environment variable");
        }

        return GoogleCredentials.fromStream(
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))
        ).createScoped(Collections.singleton("https://www.googleapis.com/auth/spreadsheets.readonly"));
    }

}
