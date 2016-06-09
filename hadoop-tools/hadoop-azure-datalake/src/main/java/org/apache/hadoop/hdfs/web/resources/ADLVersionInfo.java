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
 *
 */

package org.apache.hadoop.hdfs.web.resources;

import org.apache.hadoop.hdfs.web.ADLConfKeys;

import java.util.regex.Pattern;

/**
 * Capture ADL Jar version information. Require for debugging and analysis
 * purpose in the backend.
 */
public class ADLVersionInfo extends StringParam {
  /**
   * Parameter name.
   */
  public static final String NAME = ADLConfKeys.ADL_WEBSDK_VERSION_KEY;

  private static final StringParam.Domain DOMAIN = new StringParam.Domain(NAME,
      Pattern.compile(".+"));

  /**
   * Constructor.
   * @param featureSetVersion Enabled featured information
   */
  public ADLVersionInfo(String featureSetVersion) {
    super(DOMAIN, featureSetVersion);
  }

  @Override
  public final String getName() {
    return NAME;
  }
}
