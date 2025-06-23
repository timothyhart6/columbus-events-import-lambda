package org.columbusEventsSyncLambda;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GoogleSheetReaderTest {
    @Mock
//            (answer = Answers.RETURNS_DEEP_STUBS)
    HttpRequestFactory httpRequestFactory;
    @Mock
    HttpResponse httpResponse;

    @Test
    public void fetchSheetDataReturnsAString() throws IOException {
        GoogleSheetReader googleSheetReader = new GoogleSheetReader(httpRequestFactory);
        when(httpRequestFactory.buildGetRequest(any(GenericUrl.class)).execute()).thenReturn(httpResponse);
        when(httpResponse.parseAsString()).thenReturn("{ \"values\": [[\"A\", \"B\"]] }");

        String result = googleSheetReader.fetchSheetData("fakeId", "meaningless range");

        assertEquals("{ \"values\": [[\"A\", \"B\"]] }", result);
    }

    @Test
    void fetchSheetDataHandlesNullRequestFactory() throws IOException {
        //Unit test will not have credentials, which creates behavior needed for this test
        GoogleSheetReader reader = new GoogleSheetReader();

        String result = reader.fetchSheetData("123", "range");

        assertEquals("", result);
    }

    @Test
    void fetchSheetDataHandlesIOException() throws IOException {
        GoogleSheetReader reader = new GoogleSheetReader(httpRequestFactory);

        when(httpRequestFactory.buildGetRequest(any(GenericUrl.class))).thenThrow(new IOException("Fake IO error"));

        String result = reader.fetchSheetData("invalid", "range");

        assertEquals("", result);
    }

    @Test
    void fetchSheetDataHandlesParseFailure() throws IOException {
        when(httpRequestFactory.buildGetRequest(any(GenericUrl.class)).execute()).thenReturn(httpResponse);
        when(httpResponse.parseAsString()).thenThrow(new IOException("Parsing failed"));

        GoogleSheetReader reader = new GoogleSheetReader(httpRequestFactory);
        String result = reader.fetchSheetData("id", "A2:D");

        assertEquals("", result);
    }

}
