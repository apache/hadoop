package org.apache.hadoop.fs.azurebfs.services;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsTestWithTimeout;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.services.HttpOperationType.APACHE_HTTP_CLIENT;
import static org.apache.hadoop.fs.azurebfs.services.HttpOperationType.JDK_HTTP_URL_CONNECTION;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;


public class TestApacheHttpClientFallback extends AbstractAbfsTestWithTimeout {

  public TestApacheHttpClientFallback() throws Exception {
    super();
  }

  @Test
  public void testMultipleFailureLeadToFallback()
      throws Exception {
    TracingContext tc = Mockito.mock(TracingContext.class);
    Mockito.doNothing().when(tc).constructHeader(Mockito.any(HttpOperation.class), Mockito.nullable(String.class), Mockito.nullable(String.class));
    for (int i = 0; i < 5; i++) {
      AbfsRestOperation op = getMockRestOperation(APACHE_HTTP_CLIENT, false);
      op.execute(tc);
    }
    intercept(IOException.class, () -> {
      getMockRestOperation(APACHE_HTTP_CLIENT, true).execute(tc);
    });
    getMockRestOperation(JDK_HTTP_URL_CONNECTION, false).execute(tc);
  }

  private AbfsRestOperation getMockRestOperation(final HttpOperationType httpOperationType,
      final boolean shouldExceptionBeThrown) throws IOException {
    AbfsConfiguration configuration = Mockito.mock(AbfsConfiguration.class);
    Mockito.doReturn(APACHE_HTTP_CLIENT)
        .when(configuration)
        .getPreferredHttpOperationType();
    AbfsClient client = Mockito.mock(AbfsClient.class);
    Mockito.doReturn(Mockito.mock(ExponentialRetryPolicy.class)).when(client).getExponentialRetryPolicy();

    AbfsRetryPolicy retryPolicy = Mockito.mock(AbfsRetryPolicy.class);
    Mockito.doReturn(retryPolicy).when(client).getRetryPolicy(Mockito.nullable(String.class));
    Mockito.doReturn(false).when(retryPolicy).shouldRetry(Mockito.anyInt(), Mockito.nullable(Integer.class));

    AbfsThrottlingIntercept abfsThrottlingIntercept = Mockito.mock(AbfsThrottlingIntercept.class);
    Mockito.doNothing().when(abfsThrottlingIntercept).updateMetrics(Mockito.any(AbfsRestOperationType.class), Mockito.any(HttpOperation.class));
    Mockito.doNothing().when(abfsThrottlingIntercept).sendingRequest(Mockito.any(AbfsRestOperationType.class), Mockito.nullable(AbfsCounters.class));
    Mockito.doReturn(abfsThrottlingIntercept).when(client).getIntercept();


    AbfsRestOperation op = Mockito.spy(new AbfsRestOperation(
        AbfsRestOperationType.ReadFile,
        client,
        AbfsHttpConstants.HTTP_METHOD_GET,
        new URL("http://localhost"),
        new ArrayList<>(),
        null,
        configuration,
        "clientId"
    ));

    Mockito.doReturn(null).when(op).getClientLatency();

    Mockito.doReturn(Mockito.mock(AbfsHttpOperation.class)).when(op).createAbfsHttpOperation();
    Mockito.doReturn(Mockito.mock(AbfsAHCHttpOperation.class)).when(op).createAbfsAHCHttpOperation();

    Mockito.doAnswer(answer -> {
      return answer.getArgument(0);
    }).when(op).createNewTracingContext(Mockito.nullable(TracingContext.class));

    Mockito.doNothing().when(op).signRequest(Mockito.any(HttpOperation.class), Mockito.anyInt());

    Mockito.doAnswer(answer -> {
      HttpOperation operation = Mockito.spy(
          (HttpOperation) answer.callRealMethod());
      Assertions.assertThat(operation).isInstanceOf(
          httpOperationType == APACHE_HTTP_CLIENT
              ? AbfsAHCHttpOperation.class
              : AbfsHttpOperation.class);
      Mockito.doReturn(200).when(operation).getStatusCode();
      if (shouldExceptionBeThrown) {
        Mockito.doThrow(new IOException("Test Exception"))
            .when(operation)
            .processResponse(Mockito.nullable(byte[].class), Mockito.anyInt(),
                Mockito.anyInt());
      } else {
        Mockito.doNothing()
            .when(operation)
            .processResponse(Mockito.any(byte[].class), Mockito.anyInt(),
                Mockito.anyInt());
      }
      Mockito.doCallRealMethod().when(operation).incrementServerCall();
      Mockito.doCallRealMethod().when(operation).registerIOException();
      return operation;
    }).when(op).createHttpOperation();
    return op;
  }
}
