/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.IntegerConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.BooleanConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.StringConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.LongConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.annotations.ConfigurationValidationAnnotations.Base64StringConfigurationValidatorAnnotation;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.KeyProviderException;
import org.apache.hadoop.fs.azurebfs.utils.Base64;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_SSL_CHANNEL_MODE_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations.*;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.security.ssl.DelegatingSSLSocketFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

/**
 * Test ConfigurationServiceFieldsValidation.
 */
public class TestAbfsConfigurationFieldsValidation {
  private AbfsConfiguration abfsConfiguration;

  private static final String INT_KEY = "intKey";
  private static final String LONG_KEY = "longKey";
  private static final String STRING_KEY = "stringKey";
  private static final String BASE64_KEY = "base64Key";
  private static final String BOOLEAN_KEY = "booleanKey";
  private static final int DEFAULT_INT = 4194304;
  private static final int DEFAULT_LONG = 4194304;

  private static final int TEST_INT = 1234565;
  private static final int TEST_LONG = 4194304;

  private final String accountName;
  private final String encodedString;
  private final String encodedAccountKey;

  @IntegerConfigurationValidatorAnnotation(ConfigurationKey = INT_KEY,
      MinValue = Integer.MIN_VALUE,
      MaxValue = Integer.MAX_VALUE,
      DefaultValue = DEFAULT_INT)
  private int intField;

  @LongConfigurationValidatorAnnotation(ConfigurationKey = LONG_KEY,
      MinValue = Long.MIN_VALUE,
      MaxValue = Long.MAX_VALUE,
      DefaultValue = DEFAULT_LONG)
  private int longField;

  @StringConfigurationValidatorAnnotation(ConfigurationKey = STRING_KEY,
      DefaultValue = "default")
  private String stringField;

  @Base64StringConfigurationValidatorAnnotation(ConfigurationKey = BASE64_KEY,
      DefaultValue = "base64")
  private String base64Field;

  @BooleanConfigurationValidatorAnnotation(ConfigurationKey = BOOLEAN_KEY,
      DefaultValue = false)
  private boolean boolField;

  public TestAbfsConfigurationFieldsValidation() throws Exception {
    super();
    this.accountName = "testaccount1.blob.core.windows.net";
    this.encodedString = Base64.encode("base64Value".getBytes(StandardCharsets.UTF_8));
    this.encodedAccountKey = Base64.encode("someAccountKey".getBytes(StandardCharsets.UTF_8));
    Configuration configuration = new Configuration();
    configuration.addResource(TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME);
    configuration.set(INT_KEY, "1234565");
    configuration.set(LONG_KEY, "4194304");
    configuration.set(STRING_KEY, "stringValue");
    configuration.set(BASE64_KEY, encodedString);
    configuration.set(BOOLEAN_KEY, "true");
    configuration.set(ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME + "." + accountName, this.encodedAccountKey);
    abfsConfiguration = new AbfsConfiguration(configuration, accountName);
  }

  @Test
  public void testValidateFunctionsInConfigServiceImpl() throws Exception {
    Field[] fields = this.getClass().getDeclaredFields();
    for (Field field : fields) {
      field.setAccessible(true);
      if (field.isAnnotationPresent(IntegerConfigurationValidatorAnnotation.class)) {
        assertThat(abfsConfiguration.validateInt(field)).isEqualTo(TEST_INT);
      } else if (field.isAnnotationPresent(LongConfigurationValidatorAnnotation.class)) {
        assertThat(abfsConfiguration.validateLong(field)).isEqualTo(DEFAULT_LONG);
      } else if (field.isAnnotationPresent(StringConfigurationValidatorAnnotation.class)) {
        assertThat(abfsConfiguration.validateString(field)).isEqualTo("stringValue");
      } else if (field.isAnnotationPresent(Base64StringConfigurationValidatorAnnotation.class)) {
        assertThat(abfsConfiguration.validateBase64String(field)).isEqualTo(this.encodedString);
      } else if (field.isAnnotationPresent(BooleanConfigurationValidatorAnnotation.class)) {
        assertThat(abfsConfiguration.validateBoolean(field)).isEqualTo(true);
      }
    }
  }

  @Test
  public void testConfigServiceImplAnnotatedFieldsInitialized() throws Exception {
    // test that all the ConfigurationServiceImpl annotated fields have been initialized in the constructor
    assertThat(abfsConfiguration.getWriteBufferSize())
            .describedAs("Default value of write buffer size should be initialized")
            .isEqualTo(DEFAULT_WRITE_BUFFER_SIZE);
    assertThat(abfsConfiguration.getReadBufferSize())
            .describedAs("Default value of read buffer size should be initialized")
            .isEqualTo(DEFAULT_READ_BUFFER_SIZE);
    assertThat(abfsConfiguration.getMinBackoffIntervalMilliseconds())
            .describedAs("Default value of min backoff interval should be initialized")
            .isEqualTo(DEFAULT_MIN_BACKOFF_INTERVAL);
    assertThat(abfsConfiguration.getMaxBackoffIntervalMilliseconds())
            .describedAs("Default value of max backoff interval should be initialized")
            .isEqualTo(DEFAULT_MAX_BACKOFF_INTERVAL);
    assertThat(abfsConfiguration.getBackoffIntervalMilliseconds())
            .describedAs("Default value of backoff interval should be initialized")
            .isEqualTo(DEFAULT_BACKOFF_INTERVAL);
    assertThat(abfsConfiguration.getMaxIoRetries())
            .describedAs("Default value of max number of retries should be initialized")
            .isEqualTo(DEFAULT_MAX_RETRY_ATTEMPTS);
    assertThat(abfsConfiguration.getAzureBlockSize())
            .describedAs("Default value of azure block size should be initialized")
            .isEqualTo(MAX_AZURE_BLOCK_SIZE);
    assertThat(abfsConfiguration.getAzureBlockLocationHost())
            .describedAs("Default value of azure block location host should be initialized")
            .isEqualTo(AZURE_BLOCK_LOCATION_HOST_DEFAULT);
    assertThat(abfsConfiguration.getReadAheadRange())
            .describedAs("Default value of read ahead range should be initialized")
            .isEqualTo(DEFAULT_READ_AHEAD_RANGE);
    assertThat(abfsConfiguration.getHttpConnectionTimeout())
            .describedAs("Default value of http connection timeout should be initialized")
            .isEqualTo(DEFAULT_HTTP_CONNECTION_TIMEOUT);
    assertThat(abfsConfiguration.getHttpReadTimeout())
            .describedAs("Default value of http read timeout should be initialized")
            .isEqualTo(DEFAULT_HTTP_READ_TIMEOUT);
  }

  @Test
  public void testConfigBlockSizeInitialized() throws Exception {
    // test the block size annotated field has been initialized in the constructor
    assertThat(abfsConfiguration.getAzureBlockSize())
            .describedAs("Default value of max azure block size should be initialized")
            .isEqualTo(MAX_AZURE_BLOCK_SIZE);
  }

  @Test
  public void testGetAccountKey() throws Exception {
    String accountKey = abfsConfiguration.getStorageAccountKey();
    assertThat(accountKey).describedAs("Account Key should be initialized in configs")
            .isEqualTo(this.encodedAccountKey);
  }

  @Test
  public void testGetAccountKeyWithNonExistingAccountName() throws Exception {
      Assertions.assertThrows(KeyProviderException.class, () -> {
          Configuration configuration = new Configuration();
          configuration.addResource(TestConfigurationKeys.TEST_CONFIGURATION_FILE_NAME);
          configuration.unset(ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME);
          AbfsConfiguration abfsConfig = new AbfsConfiguration(configuration, "bogusAccountName");
          abfsConfig.getStorageAccountKey();
      });
  }

  @Test
  public void testSSLSocketFactoryConfiguration()
      throws InvalidConfigurationValueException, IllegalAccessException, IOException {
    assertThat(abfsConfiguration.getPreferredSSLFactoryOption())
            .describedAs("By default SSL Channel Mode should be Default")
            .isEqualTo(DelegatingSSLSocketFactory.SSLChannelMode.Default);
    assertThat(abfsConfiguration.getPreferredSSLFactoryOption())
            .describedAs("By default SSL Channel Mode should be Default")
            .isNotEqualTo(DelegatingSSLSocketFactory.SSLChannelMode.Default_JSSE);
    assertThat(abfsConfiguration.getPreferredSSLFactoryOption())
            .describedAs("By default SSL Channel Mode should be Default")
            .isNotEqualTo(DelegatingSSLSocketFactory.SSLChannelMode.OpenSSL);
    Configuration configuration = new Configuration();
    configuration.setEnum(FS_AZURE_SSL_CHANNEL_MODE_KEY, DelegatingSSLSocketFactory.SSLChannelMode.Default_JSSE);
    AbfsConfiguration localAbfsConfiguration = new AbfsConfiguration(configuration, accountName);
    assertThat(localAbfsConfiguration.getPreferredSSLFactoryOption())
            .describedAs("SSL Channel Mode should be Default_JSSE as set")
            .isEqualTo(DelegatingSSLSocketFactory.SSLChannelMode.Default_JSSE);

    configuration = new Configuration();
    configuration.setEnum(FS_AZURE_SSL_CHANNEL_MODE_KEY, DelegatingSSLSocketFactory.SSLChannelMode.OpenSSL);
    localAbfsConfiguration = new AbfsConfiguration(configuration, accountName);
    assertThat(localAbfsConfiguration.getPreferredSSLFactoryOption())
            .describedAs("SSL Channel Mode should be OpenSSL as set")
            .isEqualTo(DelegatingSSLSocketFactory.SSLChannelMode.OpenSSL);
  }

  public static AbfsConfiguration updateRetryConfigs(AbfsConfiguration abfsConfig,
      int retryCount,
      int backoffTime) {
    abfsConfig.setMaxIoRetries(retryCount);
    abfsConfig.setMaxBackoffIntervalMilliseconds(backoffTime);
    return abfsConfig;
  }
}
