package org.apache.hadoop.fs.azurebfs;

import org.junit.Assume;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_BLOB_ENDPOINT;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DNS_PREFIX;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.WASB_DNS_PREFIX;

public class ITestAzureBlobFileSystemBlobConfig extends AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemBlobConfig() throws Exception {
    super();
  }

  @Test
  public void test() throws Exception {
    testForBlobEndpointConfig(false, false);
  }

  private void testForBlobEndpointConfig(Boolean configVal, Boolean dfsEndpoint) throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
    Assume.assumeFalse(fs.getIsNamespaceEnabled(Mockito.mock(TracingContext.class)));
    if(dfsEndpoint) {
      String url = getTestUrl();
      if(url.contains(WASB_DNS_PREFIX)) {
        url = url.replace(WASB_DNS_PREFIX, ABFS_DNS_PREFIX);
        getRawConfiguration().set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, url);
      }
    } else {
      String url = getTestUrl();
      if(url.contains(ABFS_DNS_PREFIX)) {
        url = url.replace(ABFS_DNS_PREFIX, WASB_DNS_PREFIX);
        getRawConfiguration().set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, url);
      }
    }
    getRawConfiguration().set(FS_AZURE_ENABLE_BLOB_ENDPOINT, configVal.toString());
    fs = (AzureBlobFileSystem) FileSystem.newInstance(getRawConfiguration());
    int a =1;
    a++;
  }
}
