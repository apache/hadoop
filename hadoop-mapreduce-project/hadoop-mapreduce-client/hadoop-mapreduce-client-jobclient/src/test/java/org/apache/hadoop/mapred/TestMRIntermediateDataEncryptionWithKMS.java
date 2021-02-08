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
package org.apache.hadoop.mapred;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.JavaKeyStoreProvider;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.SpillHdfsKMSKeyProvider;
import org.apache.hadoop.mapreduce.SpillKeyProvider;

/**
 * {@inheritdoc}
 * Extends {@link TestMRIntermediateDataEncryption} to test the intermediate
 * encryption with KMS Key provider.
 */
public class TestMRIntermediateDataEncryptionWithKMS extends
    TestMRIntermediateDataEncryption {
  /**
   * Initialized the parametrized JUnit test.
   *
   * @param testName    the name of the unit test to be executed.
   * @param mappers     number of mappers in the tests.
   * @param reducers    number of the reducers.
   * @param uberEnabled boolean flag for isUber
   */
  public TestMRIntermediateDataEncryptionWithKMS(String testName, int mappers,
      int reducers, boolean uberEnabled) {
    super(testName, mappers, reducers, uberEnabled);
  }

  /**
   * {@inheritdoc}.
   */
  @Override
  protected void setupSpillKeyProviderConf(JobConf jobConf) throws Exception {
    super.setupSpillKeyProviderConf(jobConf);
    String encryptionKeyName =
        jobConf.get(
            MRJobConfig.MR_ENCRYPTED_INTERMEDIATE_DATA_KEYPROVIDER_KEY_NAME,
            MRJobConfig
                .DEFAULT_MR_ENCRYPTED_INTERMEDIATE_DATA_KEYPROVIDER_KEY_NAME);
    try (KeyProvider keyProvider =
             getDFSCluster().getNameNode().getNamesystem().getProvider()) {
      KeyProvider.Options options = KeyProvider.options(jobConf);
      if (keyProvider.getCurrentKey(encryptionKeyName) == null) {
        keyProvider.createKey(encryptionKeyName, options);
        keyProvider.flush();
      }
    }
  }

  /**
   * {@inheritdoc}.
   */
  @Override
  protected Configuration setupClustersConf() {
    Configuration conf = super.setupClustersConf();
    FileSystemTestHelper fsHelper = new FileSystemTestHelper();
    File kmsDir = new File(fsHelper.getTestRootDir()).getAbsoluteFile();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        getKeyProviderURI(kmsDir));
    return conf;
  }

  /**
   * {@inheritdoc}.
   */
  @Override
  protected Class<? extends SpillKeyProvider> getSpillKeyKlass() {
    return SpillHdfsKMSKeyProvider.class;
  }

  private static String getKeyProviderURI(File testRootDir) {
    return JavaKeyStoreProvider.SCHEME_NAME + "://file" +
        new Path(testRootDir.toString(), "test.jks").toUri();
  }
}
