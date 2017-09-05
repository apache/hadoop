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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.crypto.key.kms.server.KMSACLs;
import org.apache.hadoop.crypto.key.kms.server.KMSConfiguration;
import org.apache.hadoop.crypto.key.kms.server.KMSWebApp;
import org.apache.hadoop.crypto.key.kms.server.MiniKMS;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

/**
 * Test class for re-encryption with minikms.
 */
public class TestReencryptionWithKMS extends TestReencryption{

  private MiniKMS miniKMS;
  private String kmsDir;

  @Override
  protected String getKeyProviderURI() {
    return KMSClientProvider.SCHEME_NAME + "://" +
        miniKMS.getKMSUrl().toExternalForm().replace("://", "@");
  }

  @Before
  public void setup() throws Exception {
    kmsDir = "target/test-classes/" + UUID.randomUUID().toString();
    final File dir = new File(kmsDir);
    assertTrue(dir.mkdirs());
    MiniKMS.Builder miniKMSBuilder = new MiniKMS.Builder();
    miniKMS = miniKMSBuilder.setKmsConfDir(dir).build();
    miniKMS.start();
    super.setup();
  }

  @After
  public void teardown() {
    super.teardown();
    if (miniKMS != null) {
      miniKMS.stop();
    }
  }

  @Override
  protected void setProvider() {
  }

  @Test
  public void testReencryptionKMSACLs() throws Exception {
    final Path aclPath = new Path(kmsDir, KMSConfiguration.KMS_ACLS_XML);
    final Configuration acl = new Configuration(false);
    acl.addResource(aclPath);
    // should not require any of the get ACLs.
    acl.set(KMSACLs.Type.GET.getBlacklistConfigKey(), "*");
    acl.set(KMSACLs.Type.GET_KEYS.getBlacklistConfigKey(), "*");
    final File kmsAcl = new File(aclPath.toString());
    assertTrue(kmsAcl.exists());
    try (Writer writer = new FileWriter(kmsAcl)) {
      acl.writeXml(writer);
    }
    KMSWebApp.getACLs().run();
    testReencryptionBasic();
  }
}
