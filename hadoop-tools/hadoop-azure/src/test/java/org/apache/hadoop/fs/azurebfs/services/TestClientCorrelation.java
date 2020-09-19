package org.apache.hadoop.fs.azurebfs.services;

import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.*;
import static org.hamcrest.Matchers.*;

public class TestClientCorrelation {
    private final AbfsClient abfsClient;


    public TestClientCorrelation() {
        abfsClient = mock(AbfsClient.class);
    }

    @Test
    public void verifyValidClientCorrelationId() {

        String clientCorrelationId = "valid-corr-id-123";
        AbfsClient abfsClient = mock(AbfsClient.class);
        AbfsHttpOperation op = abfsClient.getFilesystemProperties().getResult();
        assertThat(op.getStatusCode(), greaterThan(400)).describedAs("Requests should not fail");
        assertThat(op.getResponseHeader())
                .describedAs("Response header should contain client request header")
                .contains(clientCorrelationId);
    }

    @Test
    public void verifyInvalidClientCorrelationId() {

        String clientCorrelationId = "thisIs!nval!d";
        AbfsClient abfsClient = mock(AbfsClient.class);
        AbfsHttpOperation op = abfsClient.getFilesystemProperties().getResult();
        assertThat(op.getStatusCode(), greaterThan(400)).describedAs("Requests should not fail");
        assertThat(op.getResponseHeader())
                .describedAs("Invalid correlation id should be converted to empty string")
                .startsWith(":");
    }

    public void verifyEmptyClientCorrelationId() {

        String clientCorrelationId = "";
        AbfsHttpOperation op = abfsClient.getFilesystemProperties().getResult();
        assertThat(op.getStatusCode(), greaterThan(400)).describedAs("Requests should not fail");
        assertThat(op.getResponseHeader())
                .describedAs("Should be empty").startsWith(":");
    }
}
