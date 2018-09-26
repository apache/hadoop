/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.aliyun.oss;

import com.aliyun.oss.common.auth.Credentials;
import com.aliyun.oss.common.auth.InvalidCredentialsException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.aliyun.oss.contract.AliyunOSSContract;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.fs.aliyun.oss.Constants.ACCESS_KEY_ID;
import static org.apache.hadoop.fs.aliyun.oss.Constants.ACCESS_KEY_SECRET;
import static org.apache.hadoop.fs.aliyun.oss.Constants.SECURITY_TOKEN;

/**
 * Tests use of temporary credentials (for example, Aliyun STS & Aliyun OSS).
 * This test extends a class that "does things to the root directory", and
 * should only be used against transient filesystems where you don't care about
 * the data.
 */
public class TestAliyunCredentials extends AbstractFSContractTestBase {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new AliyunOSSContract(conf);
  }

  @Test
  public void testCredentialMissingAccessKeyId() throws Throwable {
    Configuration conf = new Configuration();
    conf.set(ACCESS_KEY_ID, "");
    conf.set(ACCESS_KEY_SECRET, "accessKeySecret");
    conf.set(SECURITY_TOKEN, "token");
    validateCredential(conf);
  }

  @Test
  public void testCredentialMissingAccessKeySecret() throws Throwable {
    Configuration conf = new Configuration();
    conf.set(ACCESS_KEY_ID, "accessKeyId");
    conf.set(ACCESS_KEY_SECRET, "");
    conf.set(SECURITY_TOKEN, "token");
    validateCredential(conf);
  }

  private void validateCredential(Configuration conf) {
    try {
      AliyunCredentialsProvider provider
          = new AliyunCredentialsProvider(conf);
      Credentials credentials = provider.getCredentials();
      fail("Expected a CredentialInitializationException, got " + credentials);
    } catch (InvalidCredentialsException expected) {
      // expected
    } catch (IOException e) {
      fail("Unexpected exception.");
    }
  }
}
