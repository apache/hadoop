/*
 * ByteDance Volcengine EMR, Copyright 2022.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.object.tos.auth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.tosfs.conf.TosKeys.FS_TOS_ACCESS_KEY_ID;
import static org.apache.hadoop.fs.tosfs.conf.TosKeys.FS_TOS_SECRET_ACCESS_KEY;

public abstract class AbstractCredentialsProvider implements CredentialsProvider {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractCredentialsProvider.class);

  protected volatile ExpireableCredential credential;
  private Configuration conf;
  private String bucket;

  @Override
  public void initialize(Configuration config, String bucketName) {
    this.conf = config;
    this.bucket = bucketName;
  }

  /**
   * throw exception if no valid credential found, the response credential is not null.
   *
   * @return credential
   */
  @Override
  public ExpireableCredential credential() {
    if (credential == null || credential.isExpired()) {
      synchronized (this) {
        if (credential == null || credential.isExpired()) {
          LOG.debug("Credential expired, create a new credential");
          ExpireableCredential cred = createCredential();
          Preconditions.checkNotNull(cred.getAccessKeyId(), "%s cannot be null",
              FS_TOS_ACCESS_KEY_ID);
          Preconditions.checkNotNull(cred.getAccessKeySecret(), "%s cannot be null",
              FS_TOS_SECRET_ACCESS_KEY);
          credential = cred;
        }
      }
    }
    return credential;
  }

  public Configuration conf() {
    return conf;
  }

  public String bucket() {
    return bucket;
  }

  /**
   * Create expireable credential.
   *
   * throw exception if not credential found.
   * @return expireable credential.
   */
  protected abstract ExpireableCredential createCredential();
}
