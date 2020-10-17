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
package org.apache.hadoop.hdfs.web.resources;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_WEBHDFS_USER_PATTERN_DEFAULT;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.text.MessageFormat;
import java.util.regex.Pattern;

/** User parameter. */
public class UserParam extends StringParam {
  /** Parameter name. */
  public static final String NAME = "user.name";
  /** Default parameter value. */
  public static final String DEFAULT = "";

  private static Domain domain = new Domain(NAME,
      Pattern.compile(DFS_WEBHDFS_USER_PATTERN_DEFAULT));

  @VisibleForTesting
  public static Domain getUserPatternDomain() {
    return domain;
  }

  @VisibleForTesting
  public static void setUserPatternDomain(Domain dm) {
    domain = dm;
  }

  public static void setUserPattern(String pattern) {
    domain = new Domain(NAME, Pattern.compile(pattern));
  }

  private static String validateLength(String str) {
    if (str == null) {
      throw new IllegalArgumentException(
        MessageFormat.format("Parameter [{0}], cannot be NULL", NAME));
    }
    int len = str.length();
    if (len < 1) {
      throw new IllegalArgumentException(MessageFormat.format(
        "Parameter [{0}], it's length must be at least 1", NAME));
    }
    return str;
  }

  /**
   * Constructor.
   * @param str a string representation of the parameter value.
   */
  public UserParam(final String str) {
    super(domain, str == null ||
        str.equals(DEFAULT) ? null : validateLength(str));
  }

  /**
   * Construct an object from a UGI.
   */
  public UserParam(final UserGroupInformation ugi) {
    this(ugi.getShortUserName());
  }

  @Override
  public String getName() {
    return NAME;
  }
}
