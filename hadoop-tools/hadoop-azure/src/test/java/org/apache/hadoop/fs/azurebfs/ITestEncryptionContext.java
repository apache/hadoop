package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.extensions.MockDelegationSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.extensions.MockEncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE;

public class ITestEncryptionContext extends AbstractAbfsIntegrationTest {
  public ITestEncryptionContext() throws Exception {
    String encryptionContextProvider = getRawConfiguration().get(
        FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE);
    Assume.assumeTrue(MockEncryptionContextProvider.class.getCanonicalName()
        .equals(encryptionContextProvider));
  }

  private AzureBlobFileSystem getAbfsEncrypted() throws Exception {
    Configuration configuration = getRawConfiguration();
    configuration.set(FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE, "MockEncryptionContextProvider");
    return getFileSystem(configuration);
  }

  @Test
  public void testCreateFile() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();
//    AzureBlobFileSystem fs = getAbfsEncrypted();
    Path testPath = path("createTest");
    FSDataOutputStream out = fs.create(testPath);
    Assertions.assertThat(((AbfsOutputStream) out.getWrappedStream())
            .isEncryptionHeadersCached()).isTrue();
  }
}
