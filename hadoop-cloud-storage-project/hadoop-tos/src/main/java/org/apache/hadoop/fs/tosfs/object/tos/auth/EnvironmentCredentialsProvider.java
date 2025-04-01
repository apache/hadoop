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

import org.apache.hadoop.fs.tosfs.object.tos.TOS;

public class EnvironmentCredentialsProvider extends AbstractCredentialsProvider {

  public static final String NAME = EnvironmentCredentialsProvider.class.getName();

  @Override
  protected ExpireableCredential createCredential() {
    return new ExpireableCredential(
        System.getenv(TOS.ENV_TOS_ACCESS_KEY_ID),
        System.getenv(TOS.ENV_TOS_SECRET_ACCESS_KEY),
        System.getenv(TOS.ENV_TOS_SESSION_TOKEN),
        Long.MAX_VALUE
    );
  }
}
