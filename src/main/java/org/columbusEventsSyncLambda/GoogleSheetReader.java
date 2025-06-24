package org.columbusEventsSyncLambda;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.api.client.http.javanet.NetHttpTransport;
import org.columbusEventsSyncLambda.models.Event;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    public List<Event> fetchEvents(String sheetId, String range) {
        List<Event> events = new ArrayList<>();
        try {
            if (requestFactory == null) {
                log.warn("HttpRequestFactory is not initialized.");
                return events;
            }
            String url = String.format(
                   "https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s",
                   sheetId, range);
            GenericUrl genericUrl = new GenericUrl(url);
            HttpResponse response = requestFactory.buildGetRequest(genericUrl).execute();
            String json = response.parseAsString();
            events = convertStringToListOfEvents(json);
        } catch (IOException e) {
            log.warn("No google sheet data available: {}", e.getMessage());
        }
        return events;
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

    List<Event> convertStringToListOfEvents(String json) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json);
        JsonNode rows = root.path("values");
        List<Event> events = new ArrayList<>();
        for (JsonNode row : rows) {
            String locationName = row.path(0).asText("");
            String eventName = row.path(1).asText("");
            String date = row.path(2).asText("");
            String time = row.path(3).asText(""); // optional
            boolean isBadTraffic = row.path(4).asBoolean(true);
            boolean isDesiredEvent = row.path(5).asBoolean(true);

            events.add(Event.builder()
                    .locationName(locationName)
                    .eventName(eventName)
                    .date(date)
                    .time(time)
                    .isBadTraffic(isBadTraffic)
                    .isDesiredEvent(isDesiredEvent)
                    .build());
        }
        return events;
    }

}