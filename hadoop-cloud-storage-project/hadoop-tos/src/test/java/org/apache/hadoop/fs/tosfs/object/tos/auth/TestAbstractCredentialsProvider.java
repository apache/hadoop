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
import org.apache.hadoop.fs.tosfs.object.tos.TOS;
import org.apache.hadoop.fs.tosfs.util.TestUtility;

public abstract class TestAbstractCredentialsProvider {
  private String envAccessKeyId;
  private String envSecretAccessKey;
  private String envSessionToken;

  protected Configuration getConf() {
    return new Configuration();
  }

  protected void saveOsCredEnv() {
    if (StringUtils.isNotEmpty(System.getenv(TOS.ENV_TOS_ACCESS_KEY_ID))) {
      envAccessKeyId = System.getenv(TOS.ENV_TOS_ACCESS_KEY_ID);
    }

    if (StringUtils.isNotEmpty(System.getenv(TOS.ENV_TOS_SECRET_ACCESS_KEY))) {
      envSecretAccessKey = System.getenv(TOS.ENV_TOS_SECRET_ACCESS_KEY);
    }

    if (StringUtils.isNotEmpty(System.getenv(TOS.ENV_TOS_SESSION_TOKEN))) {
      envSessionToken = System.getenv(TOS.ENV_TOS_SESSION_TOKEN);
    }
  }

  protected void resetOsCredEnv() {
    resetOsCredEnv(TOS.ENV_TOS_ACCESS_KEY_ID, envAccessKeyId);
    resetOsCredEnv(TOS.ENV_TOS_SECRET_ACCESS_KEY, envSecretAccessKey);
    resetOsCredEnv(TOS.ENV_TOS_SESSION_TOKEN, envSessionToken);
  }

  private void resetOsCredEnv(String key, String value) {
    if (StringUtils.isNotEmpty(value)) {
      TestUtility.setSystemEnv(key, value);
    } else {
      TestUtility.removeSystemEnv(key);
    }
  }
}
