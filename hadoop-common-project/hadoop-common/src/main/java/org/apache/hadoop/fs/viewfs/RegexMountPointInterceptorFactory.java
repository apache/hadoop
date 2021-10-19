/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The interceptor factory used to create RegexMountPoint interceptors.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
final class RegexMountPointInterceptorFactory {

  private RegexMountPointInterceptorFactory() {

  }

  /**
   * interceptorSettingsString string should be like ${type}:${string},
   * e.g. replaceresolveddstpath:word1,word2.
   *
   * @param interceptorSettingsString
   * @return Return interceptor based on setting or null on bad/unknown config.
   */
  public static RegexMountPointInterceptor create(
      String interceptorSettingsString) {
    int typeTagIndex = interceptorSettingsString
        .indexOf(RegexMountPoint.INTERCEPTOR_INTERNAL_SEP);
    if (typeTagIndex == -1 || (typeTagIndex == (
        interceptorSettingsString.length() - 1))) {
      return null;
    }
    String typeTag = interceptorSettingsString.substring(0, typeTagIndex).trim()
        .toLowerCase();
    RegexMountPointInterceptorType interceptorType =
        RegexMountPointInterceptorType.get(typeTag);
    if (interceptorType == null) {
      return null;
    }
    switch (interceptorType) {
    case REPLACE_RESOLVED_DST_PATH:
      RegexMountPointInterceptor interceptor =
          RegexMountPointResolvedDstPathReplaceInterceptor
              .deserializeFromString(interceptorSettingsString);
      return interceptor;
    default:
      // impossible now
      return null;
    }
  }
}
