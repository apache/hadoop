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
package org.apache.hadoop.hdfs.web.resources;

import org.apache.hadoop.hdfs.protocol.HdfsConstants;

/** The name space quota parameter for directory. */
public class NameSpaceQuotaParam extends LongParam {
  /** Parameter name. */
  public static final String NAME = "namespacequota";
  /** Default parameter value ({@link Long#MAX_VALUE}). */
  public static final String DEFAULT = "9223372036854775807";

  private static final Domain DOMAIN = new Domain(NAME);

  public NameSpaceQuotaParam(final Long value) {
    super(DOMAIN, value, HdfsConstants.QUOTA_RESET,
        HdfsConstants.QUOTA_DONT_SET);
  }

  public NameSpaceQuotaParam(final String str) {
    this(DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }
}
