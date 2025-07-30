package org.columbusEventsImportLambda.service.google;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.*;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.gson.GsonFactory;
import lombok.extern.slf4j.Slf4j;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.api.client.http.javanet.NetHttpTransport;
import org.columbusEventsImportLambda.models.GoogleEvent;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public class GoogleSheetService {

    HttpRequestFactory requestFactory;

    public GoogleSheetService() {
        this.requestFactory = getHttpRequestFactory();
    }

//  Used for testing purposes
    public GoogleSheetService(HttpRequestFactory requestFactory) {
        this.requestFactory = requestFactory;
    }

    public List<GoogleEvent> fetchEvents(String sheetId, String range) {
        List<GoogleEvent> events = new ArrayList<>();
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
            log.warn("Error reading from Google Sheet: {}", e.getMessage());
        }
        return events;
    }

    private HttpRequestFactory getHttpRequestFactory() {
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

    private GoogleCredentials loadGoogleCredentials() throws IOException {
        String json = System.getenv("GCP_CREDENTIALS_JSON");
        if (json == null || json.isEmpty()) {
            throw new IllegalStateException("Missing GCP_CREDENTIALS_JSON environment variable");
        }
        return GoogleCredentials.fromStream(
            new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))
        ).createScoped(Collections.singleton("https://www.googleapis.com/auth/spreadsheets.readonly"));
    }

    private List<GoogleEvent> convertStringToListOfEvents(String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json);
        List<GoogleEvent> events = new ArrayList<>();

        JsonNode values = root.get("values");
        if (values != null && values.isArray()) {
            for (int i = 0; i < values.size(); i++) {
                JsonNode row = values.get(i);
                GoogleEvent event = GoogleEvent.builder()
                        .rowNumber(i + 1 + 1) // +1 for 0-based index, +1 for header row (assumes A2:D starts at row 2)
                        .locationName(getSafeString(row, 0))
                        .eventName(getSafeString(row, 1))
                        .date(getSafeString(row, 2))
                        .time(getSafeString(row, 3))
                        .isBadTraffic(parseBoolean(row, 4))
                        .isDesiredEvent(parseBoolean(row, 5))
                        .build();
                events.add(event);
            }
        }
        return events;
    }

    private String getSafeString(JsonNode row, int index) {
        return row.has(index) ? row.get(index).asText() : "";
    }

    private boolean parseBoolean(JsonNode row, int index) {
        return row.has(index) && row.get(index).asText().equalsIgnoreCase("true");
    }

    public void deleteGoogleEvent(String spreadsheetId, int rowIndex) {
        String url = String.format(
                "https://sheets.googleapis.com/v4/spreadsheets/%s:batchUpdate",
                spreadsheetId
        );

        HttpContent content = getHttpContent(rowIndex);

        try {
            HttpRequest request = requestFactory.buildPostRequest(new GenericUrl(url), content);
            request.execute();
            log.info("Successfully deleted row at index {} from Google Sheet.", rowIndex);
        } catch (IOException e) {
            log.warn("Failed to delete row {} from Google Sheet: {}", rowIndex, e.getMessage());
        }
    }

    private static HttpContent getHttpContent(int rowIndex) {
        Map<String, Object> deleteRequest = Map.of(
            "requests", List.of(
                Map.of(
                "deleteDimension", Map.of(
                    "range", Map.of(
                        "sheetId", 0, // Defaults to first sheet
                        "dimension", "ROWS",
                        "startIndex", rowIndex,
                        "endIndex", rowIndex + 1
                        )
                    )
                )
            )
        );

        return new JsonHttpContent(GsonFactory.getDefaultInstance(), deleteRequest);
    }

}