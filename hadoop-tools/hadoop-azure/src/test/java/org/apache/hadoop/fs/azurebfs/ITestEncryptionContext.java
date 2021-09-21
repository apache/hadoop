package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.extensions.MockDelegationSASTokenProvider;
import org.apache.hadoop.fs.azurebfs.extensions.MockEncryptionContextProvider;
import org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream;
import org.apache.hadoop.test.LambdaTestUtils;
import org.assertj.core.api.Assertions;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE;
import static org.assertj.core.api.ErrorCollector.intercept;

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
            .isEncryptionAdapterCached()).isTrue();

    Configuration conf = getRawConfiguration();
    conf.unset(FS_AZURE_ENCRYPTION_CONTEXT_PROVIDER_TYPE);
    fs = getFileSystem(conf);
    AzureBlobFileSystem finalFs = fs;
    LambdaTestUtils.intercept(IOException.class, () -> finalFs.append(testPath));
    LambdaTestUtils.intercept(IOException.class, () -> finalFs.open(testPath));
    LambdaTestUtils.intercept(IOException.class, () -> finalFs.append(testPath));
    LambdaTestUtils.intercept(IOException.class,
        () -> finalFs.setXAttr(testPath, "newAttr", new byte[]{1}));

  }
}
