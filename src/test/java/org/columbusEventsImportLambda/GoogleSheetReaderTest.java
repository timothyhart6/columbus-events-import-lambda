package org.columbusEventsImportLambda;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import org.columbusEventsImportLambda.google.GoogleSheetReader;
import org.columbusEventsImportLambda.models.Event;
import org.columbusEventsImportLambda.models.GoogleEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GoogleSheetReaderTest {
    @Mock
    (answer = Answers.RETURNS_DEEP_STUBS)
    HttpRequestFactory httpRequestFactory;
    @Mock
    HttpResponse httpResponse;

    @Test
    public void fetchEventsReturnsAString() throws IOException {
        GoogleSheetReader googleSheetReader = new GoogleSheetReader(httpRequestFactory);

        String json = "{\n" +
                "  \"range\": \"ColumbusEvents!A2:D999\",\n" +
                "  \"majorDimension\": \"ROWS\",\n" +
                "  \"values\": [\n" +
                "    [\n" +
                "      \"Convention Center\",\n" +
                "      \"The Arnold Classic\",\n" +
                "      \"02-28-2025\",\n" +
                "      \"All Day\"\n" +
                "    ]\n" +
                "  ]\n" +
                "}";

        when(httpRequestFactory.buildGetRequest(any(GenericUrl.class)).execute()).thenReturn(httpResponse);
        when(httpResponse.parseAsString()).thenReturn(json);

        Event result = googleSheetReader.fetchEvents("fakeId", "meaningless range").get(0);

        assertInstanceOf(Event.class, result);
        assertEquals("Convention Center", result.getLocationName());
        assertEquals("The Arnold Classic", result.getEventName());
        assertEquals("02-28-2025", result.getDate());
        assertEquals("All Day", result.getTime());
        assertTrue(result.isBadTraffic());
        assertTrue(result.isDesiredEvent());
    }

    @Test
    void fetchEventsHandlesNullRequestFactory() throws IOException {
        //Unit test will not have credentials, which creates behavior needed for this test
        GoogleSheetReader reader = new GoogleSheetReader();

        List<GoogleEvent> result = reader.fetchEvents("id", "A2:D");

        assertTrue(result.isEmpty());
    }

    @Test
    void fetchEventsHandlesIOException() throws IOException {
        GoogleSheetReader reader = new GoogleSheetReader(httpRequestFactory);

        when(httpRequestFactory.buildGetRequest(any(GenericUrl.class))).thenThrow(new IOException("Fake IO error"));

        List<GoogleEvent> result = reader.fetchEvents("id", "A2:D");

        assertTrue(result.isEmpty());
    }

    @Test
    void fetchEventsHandlesParseFailure() throws IOException {
        when(httpRequestFactory.buildGetRequest(any(GenericUrl.class)).execute()).thenReturn(httpResponse);
        when(httpResponse.parseAsString()).thenThrow(new IOException("Parsing failed"));

        GoogleSheetReader reader = new GoogleSheetReader(httpRequestFactory);
        List<GoogleEvent> result = reader.fetchEvents("id", "A2:D");

        assertTrue(result.isEmpty());
    }

}
