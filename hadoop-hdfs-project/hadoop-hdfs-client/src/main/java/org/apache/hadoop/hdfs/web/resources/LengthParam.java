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

/** Length parameter. */
public class LengthParam extends LongParam {
  /** Parameter name. */
  public static final String NAME = "length";
  /** Default parameter value. */
  public static final String DEFAULT = NULL;

  private static final Domain DOMAIN = new Domain(NAME);

  /**
   * Constructor.
   * @param value the parameter value.
   */
  public LengthParam(final Long value) {
    super(DOMAIN, value, 0L, null);
  }

  /**
   * Constructor.
   * @param str a string representation of the parameter value.
   */
  public LengthParam(final String str) {
    this(DOMAIN.parse(str));
  }

  @Override
  public String getName() {
    return NAME;
  }

  public long getLength() {
    Long v = getValue();
    return v == null ? -1 : v;
  }
}