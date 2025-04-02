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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.conf.TosKeys;

import static org.apache.hadoop.fs.tosfs.conf.TosKeys.FS_TOS_ACCESS_KEY_ID;
import static org.apache.hadoop.fs.tosfs.conf.TosKeys.FS_TOS_SECRET_ACCESS_KEY;
import static org.apache.hadoop.fs.tosfs.conf.TosKeys.FS_TOS_SESSION_TOKEN;

public class SimpleCredentialsProvider extends AbstractCredentialsProvider {

  public static final String NAME = SimpleCredentialsProvider.class.getName();

  @Override
  protected ExpireableCredential createCredential() {
    String accessKey =
        lookup(conf(), TosKeys.FS_TOS_BUCKET_ACCESS_KEY_ID.key(bucket()), FS_TOS_ACCESS_KEY_ID);
    String secretKey = lookup(conf(), TosKeys.FS_TOS_BUCKET_SECRET_ACCESS_KEY.key(bucket()),
        FS_TOS_SECRET_ACCESS_KEY);
    String sessionToken =
        lookup(conf(), TosKeys.FS_TOS_BUCKET_SESSION_TOKEN.key(bucket()), FS_TOS_SESSION_TOKEN);
    if (StringUtils.isEmpty(sessionToken)) {
      // This is a static ak sk configuration.
      return new ExpireableCredential(accessKey, secretKey);
    } else {
      // This is an assume role configuration. Due to the ak, sk and token won't be refreshed in
      // conf, set the expireTime to Long.MAX_VALUE.
      return new ExpireableCredential(accessKey, secretKey, sessionToken, Long.MAX_VALUE);
    }
  }

  static String lookup(Configuration conf, String key, String fallbackKey) {
    if (StringUtils.isNotEmpty(key)) {
      String dynValue = conf.get(key);
      if (StringUtils.isNotEmpty(dynValue)) {
        return dynValue;
      }
    }
    return conf.get(fallbackKey);
  }
}
