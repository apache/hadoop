package org.apache.hadoop.fs.azurebfs.services;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.assertj.core.api.Assertions.assertThat;
//import static org.assertj.core.api.Assertions.assertTrue;
import static org.hamcrest.Matchers.*;

import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;

import java.util.UUID;

public class TestClientCorrelation {
    private final AbfsHttpOperation op;

    public TestClientCorrelation() {
        op = mock(AbfsHttpOperation.class);
    }

    @Test
    public void verifyValidClientCorrelationId() {

        String clientCorrelationId = "valid-corr-id-123";
        op.getConnection().setRequestProperty(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID,
                clientCorrelationId + ":" + UUID.randomUUID().toString());
        Assert.assertTrue("Requests should not fail", op.getStatusCode() > 400);
        //assertThat("",op.getStatusCode(), greaterThan(400)).describedAs("Requests should not fail");
        assertThat(op.getResponseHeader(HttpHeaderConfigurations.X_MS_REQUEST_ID))
                .describedAs("Response header should contain client request header")
                .contains(clientCorrelationId);
    }

    @Test
    public void verifyInvalidClientCorrelationId() {

        String clientCorrelationId = "thisIs!nval!d";
        op.getConnection().setRequestProperty(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID,
                clientCorrelationId + ":" + UUID.randomUUID().toString());
        //assertTrue()
        Assert.assertTrue("Requests should not fail",op.getStatusCode() > 400);
        assertThat(op.getResponseHeader(HttpHeaderConfigurations.X_MS_REQUEST_ID))
                .describedAs("Invalid correlation id should be converted to empty string")
                .startsWith(":");
    }

    @Test
    public void verifyEmptyClientCorrelationId() {

        String clientCorrelationId = "";
        op.getConnection().setRequestProperty(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID,
                clientCorrelationId + ":" + UUID.randomUUID().toString());
        Assert.assertTrue("Requests should not fail",op.getStatusCode() > 400);
        assertThat(op.getResponseHeader(HttpHeaderConfigurations.X_MS_REQUEST_ID))
                .describedAs("Should be empty").startsWith(":");
    }
}
