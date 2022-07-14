package org.apache.hadoop.fs.azurebfs.services;

import org.junit.Test;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.DeletePath;
import java.lang.reflect.Method;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Assert;
import java.net.HttpURLConnection;

public class TestAbfsRestOperation extends
    AbstractAbfsIntegrationTest {

  public TestAbfsRestOperation() throws Exception {
  }

  @Test
  public void testDriverRetryMetrics() throws Exception {
    final AzureBlobFileSystem fs = getFileSystem();
    AbfsClient testClient = super.getAbfsClient(super.getAbfsStore(getFileSystem()));

    // Mock instance of AbfsRestOperation
    AbfsRestOperation op = TestAbfsClient.getRestOp(
        DeletePath, testClient, HTTP_METHOD_DELETE,
       TestAbfsClient.getTestUrl(testClient, "/NonExistingPath"), TestAbfsClient.getTestRequestHeaders(testClient));

    ArrayList<Integer> retryCounts = new ArrayList<>(Arrays.asList(35, 28, 31, 45, 10, 2, 9));
    int statusCode = HttpURLConnection.HTTP_UNAVAILABLE;
    Method getMetrics = AbfsRestOperation.class.getDeclaredMethod("updateDriverMetrics", int.class, int.class);
    getMetrics.setAccessible(true);
    for(int retryCount: retryCounts) {
      getMetrics.invoke(op, retryCount, statusCode);
    }
    //For retry count greater than max configured value, the request should fail
    Assert.assertEquals(testClient.getAbfsCounters().getAbfsDriverMetrics().getNumberOfRequestsFailed().toString(), "3");
    fs.close();
  }
}
