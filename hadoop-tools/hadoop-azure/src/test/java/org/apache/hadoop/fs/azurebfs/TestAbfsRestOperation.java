package org.apache.hadoop.fs.azurebfs;

import org.junit.Test;
import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_DELETE;
import static org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType.DeletePath;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.TestAbfsClient;
import java.lang.reflect.Method;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
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
    AbfsClient client = fs.getAbfsClient();

    // Mock instance of AbfsRestOperation
    AbfsRestOperation op = TestAbfsClient.getRestOp(
        DeletePath, client, HTTP_METHOD_DELETE,
       TestAbfsClient.getTestUrl(client, "/NonExistingPath"), TestAbfsClient.getTestRequestHeaders(client));

    ArrayList<Integer> retryCounts = new ArrayList<>(Arrays.asList(35, 28, 31, 45, 10, 2, 9));
    int statusCode = HttpURLConnection.HTTP_UNAVAILABLE;
    Method getMetrics = AbfsRestOperation.class.getDeclaredMethod("updateDriverMetrics", int.class, int.class);
    getMetrics.setAccessible(true);
    for(int retryCount: retryCounts) {
      getMetrics.invoke(op, retryCount, statusCode);
    }
    Assert.assertEquals(client.getAbfsCounters().getAbfsDriverMetrics().getNumberOfRequestsFailed().toString(), "3");
    fs.close();
  }
}
