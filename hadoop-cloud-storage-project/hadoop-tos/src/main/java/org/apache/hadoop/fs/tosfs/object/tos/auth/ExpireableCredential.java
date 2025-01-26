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

import com.volcengine.tos.auth.Credential;
import org.apache.hadoop.fs.tosfs.conf.TosKeys;
import org.apache.hadoop.util.Preconditions;

public class ExpireableCredential extends Credential {
  public static final int EXPIRED_INTERVAL_MILLIS = 1000 * 60; // 1 minute

  protected final long expireTimeMills;

  /**
   * The credential is never expired, default sts value is null and expired is Long.MAX_VALUE.
   *
   * @param accessKeyId     IAM AK.
   * @param accessKeySecret IAM SK.
   */
  public ExpireableCredential(
      String accessKeyId,
      String accessKeySecret) {
    this(accessKeyId, accessKeySecret, "", Long.MAX_VALUE);
  }

  /**
   * Credential that can expire.
   *
   * @param accessKeyId     IAM AK.
   * @param accessKeySecret IAM SK.
   * @param sessionToken   Session token.
   * @param expireTimeMills Session token expire time,
   *                        the default value is the request time +6H if get it from the meta
   *                        service.
   */
  public ExpireableCredential(
      String accessKeyId,
      String accessKeySecret,
      String sessionToken,
      long expireTimeMills) {
    super(accessKeyId, accessKeySecret, sessionToken);
    Preconditions.checkNotNull(accessKeyId,
        "%s cannot be null", TosKeys.FS_TOS_ACCESS_KEY_ID);
    Preconditions.checkNotNull(accessKeySecret,
        "%s cannot be null", TosKeys.FS_TOS_SECRET_ACCESS_KEY);
    Preconditions.checkArgument(expireTimeMills > 0, "expiredTime must be > 0");
    this.expireTimeMills = expireTimeMills;
  }

  public boolean isExpired() {
    return expireTimeMills - System.currentTimeMillis() <= EXPIRED_INTERVAL_MILLIS;
  }
}
