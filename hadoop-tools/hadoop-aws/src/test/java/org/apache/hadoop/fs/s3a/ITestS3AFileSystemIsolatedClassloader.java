/*
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

package org.apache.hadoop.fs.s3a;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.impl.InstantiationIOException;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * Checks that classloader isolation for loading extension classes is applied
 * correctly. Both default, true and false tests performed.
 * See {@link Constants#AWS_S3_CLASSLOADER_ISOLATION} property and
 * HADOOP-17372 and follow-up on HADOOP-18993 for more info.
 */
public class ITestS3AFileSystemIsolatedClassloader extends AbstractS3ATestBase {

  private static String customClassName = "custom.class.name";

  private static class CustomCredentialsProvider implements AwsCredentialsProvider {

    CustomCredentialsProvider() {
    }

    @Override
    public AwsCredentials resolveCredentials() {
      return null;
    }

  }

  private static class CustomClassLoader extends ClassLoader {
  }

  private final ClassLoader customClassLoader = spy(new CustomClassLoader());
  {
    try {
      doReturn(CustomCredentialsProvider.class)
          .when(customClassLoader)
          .loadClass(customClassName);
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex);
    }
  }

  private S3AFileSystem createNewTestFs(Configuration conf) throws IOException {
    S3AFileSystem fs = new S3AFileSystem();
    fs.initialize(getFileSystem().getUri(), conf);
    return fs;
  }

  /**
   * Asserts that the given assertions are valid in a new filesystem context.
   * The filesystem is created after setting the context classloader to
   * {@link ITestS3AFileSystemIsolatedClassloader#customClassLoader} in this way we are
   * able to check if the classloader is reset during the initialization or not.
   *
   * @param confToSet The configuration settings to be applied to the new filesystem.
   * @param asserts The assertions to be performed on the new filesystem.
   * @throws IOException If an I/O error occurs.
   */
  private void assertInNewFilesystem(Map<String, String> confToSet, Consumer<FileSystem> asserts)
          throws IOException {
    ClassLoader previousClassloader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(customClassLoader);
      Configuration conf = new Configuration();
      Assertions.assertThat(conf.getClassLoader()).isEqualTo(customClassLoader);
      S3ATestUtils.prepareTestConfiguration(conf);
      for (Map.Entry<String, String> e : confToSet.entrySet()) {
        conf.set(e.getKey(), e.getValue());
      }
      try (S3AFileSystem fs = createNewTestFs(conf)) {
        asserts.accept(fs);
      }
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassloader);
    }
  }

  private Map<String, String> mapOf() {
    return new HashMap<>();
  }

  private Map<String, String> mapOf(String key, String value) {
    HashMap<String, String> m = new HashMap<>();
    m.put(key, value);
    return m;
  }

  private Map<String, String> mapOf(String key1, String value1, String key2, String value2) {
    HashMap<String, String> m = new HashMap<>();
    m.put(key1, value1);
    m.put(key2, value2);
    return m;
  }

  @Test
  public void defaultIsolatedClassloader() throws Exception {
    assertInNewFilesystem(mapOf(), (fs) -> {
      Assertions.assertThat(fs.getConf().getClassLoader())
              .describedAs("The classloader used to load s3a fs extensions")
              .isNotEqualTo(Thread.currentThread().getContextClassLoader())
              .describedAs("the current classloader");

      Assertions.assertThat(fs.getConf().getClassLoader())
              .describedAs("The classloader used to load s3a fs extensions")
              .isEqualTo(fs.getClass().getClassLoader())
              .describedAs("the classloader that loaded the fs");
    });

    InstantiationIOException ex = intercept(
            InstantiationIOException.class,
            () -> assertInNewFilesystem(
                    mapOf(Constants.AWS_CREDENTIALS_PROVIDER, customClassName),
                    (fs) -> {}));

    Assertions.assertThat(ex.getCause())
            .describedAs("cause")
            .isInstanceOf(ClassNotFoundException.class)
            .hasMessageContaining(customClassName);
  }

  @Test
  public void isolatedClassloader() throws Exception {
    assertInNewFilesystem(mapOf(Constants.AWS_S3_CLASSLOADER_ISOLATION, "true"), (fs) -> {
      Assertions.assertThat(fs.getConf().getClassLoader())
              .describedAs("The classloader used to load s3a fs extensions")
              .isNotEqualTo(Thread.currentThread().getContextClassLoader())
              .describedAs("the current context classloader");

      Assertions.assertThat(fs.getConf().getClassLoader())
              .describedAs("The classloader used to load s3a fs extensions")
              .isEqualTo(fs.getClass().getClassLoader())
              .describedAs("the classloader that loaded the fs");
    });

    InstantiationIOException ex = intercept(
            InstantiationIOException.class,
            () -> assertInNewFilesystem(
                    mapOf(Constants.AWS_S3_CLASSLOADER_ISOLATION, "true",
                          Constants.AWS_CREDENTIALS_PROVIDER, customClassName),
                    (fs) -> {}));

    Assertions.assertThat(ex.getCause())
            .describedAs("cause")
            .isInstanceOf(ClassNotFoundException.class)
            .hasMessageContaining(customClassName);
  }

  @Test
  public void notIsolatedClassloader() throws IOException {
    Map<String, String> confToSet =
        mapOf(Constants.AWS_S3_CLASSLOADER_ISOLATION, "false",
              Constants.AWS_CREDENTIALS_PROVIDER, customClassName);
    assertInNewFilesystem(confToSet, (fs) -> {
      Assertions.assertThat(fs.getConf().getClassLoader())
              .describedAs("The classloader used to load s3a fs extensions")
              .isEqualTo(Thread.currentThread().getContextClassLoader())
              .describedAs("the current context classloader");

      Assertions.assertThat(fs.getConf().getClassLoader())
              .describedAs("The classloader used to load s3a fs extensions")
              .isNotEqualTo(fs.getClass().getClassLoader())
              .describedAs("the classloader that loaded the fs");

      S3AFileSystem s3a = (S3AFileSystem) fs;
      List<AwsCredentialsProvider> providers =
              s3a.getS3AInternals().shareCredentials("test").getProviders();
      Assertions.assertThat(providers)
              .describedAs("providers")
              .hasSize(1);
      Assertions.assertThat(providers.get(0))
              .describedAs("provider")
              .isInstanceOf(CustomCredentialsProvider.class);
    });
  }
}
