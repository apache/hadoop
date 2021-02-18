package org.apache.hadoop.fs.s3a;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.EncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.StaticEncryptionMaterialsProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static org.apache.hadoop.fs.s3a.S3ATestUtils.skipIfEncryptionTestsDisabled;

/**
 * Testing the S3 CSE - CUSTOM method by generating and using a Symmetric key.
 */
public class ITestS3AEncryptionCSESymmetric extends ITestS3AEncryptionCSE {

  private static final String KEY_WRAP_ALGO = "AES/GCM";
  private static final String CONTENT_ENCRYPTION_ALGO = "AES/GCM/NoPadding";

  /**
   * Custom implementation of the S3ACSEMaterialsProviderConfig class to
   * create a custom EncryptionMaterialsProvider by generating and using a
   * symmetric key.
   */
  protected static class SymmetricKeyConfig
      implements S3ACSEMaterialProviderConfig {
    @Override
    public EncryptionMaterialsProvider buildEncryptionProvider() {
      KeyGenerator symKeyGenerator;
      try {
        symKeyGenerator = KeyGenerator.getInstance("AES");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException("No algo error: ", e);
      }
      symKeyGenerator.init(256);
      SecretKey symKey = symKeyGenerator.generateKey();

      EncryptionMaterials encryptionMaterials = new EncryptionMaterials(
          symKey);

      return new StaticEncryptionMaterialsProvider(encryptionMaterials);
    }
  }

  /**
   * Create custom configs to use S3-CSE - CUSTOM method.
   *
   * @return Configuration.
   */
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    S3ATestUtils.disableFilesystemCaching(conf);
    conf.set(Constants.CLIENT_SIDE_ENCRYPTION_METHOD,
        S3AEncryptionMethods.CSE_CUSTOM.getMethod());
    conf.setClass(Constants.CLIENT_SIDE_ENCRYPTION_MATERIALS_PROVIDER,
        SymmetricKeyConfig.class, S3ACSEMaterialProviderConfig.class);
    return conf;
  }

  @Override
  protected void skipTest() {
    skipIfEncryptionTestsDisabled(getConfiguration());
  }

  @Override
  protected void assertEncrypted(Path path) throws IOException {
    ObjectMetadata md = getFileSystem().getObjectMetadata(path);

    // Assert KeyWrap Algo
    assertEquals("Key wrap algo isn't same as expected", KEY_WRAP_ALGO,
        md.getUserMetaDataOf(Headers.CRYPTO_KEYWRAP_ALGORITHM));

    // Assert Content Encryption Algo
    assertEquals("Content encryption algo isn't same as expected",
        CONTENT_ENCRYPTION_ALGO,
        md.getUserMetaDataOf(Headers.CRYPTO_CEK_ALGORITHM));
  }
}
