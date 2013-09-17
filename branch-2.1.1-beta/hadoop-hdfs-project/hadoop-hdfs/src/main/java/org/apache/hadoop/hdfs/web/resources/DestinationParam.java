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

import org.apache.hadoop.fs.Path;

/** Destination path parameter. */
public class DestinationParam extends StringParam {
  /** Parameter name. */
  public static final String NAME = "destination";
  /** Default parameter value. */
  public static final String DEFAULT = "";

  private static final Domain DOMAIN = new Domain(NAME, null);

  private static String validate(final String str) {
    if (str == null || str.equals(DEFAULT)) {
      return null;
    }
    if (!str.startsWith(Path.SEPARATOR)) {
      throw new IllegalArgumentException("Invalid parameter value: " + NAME
          + " = \"" + str + "\" is not an absolute path.");
    }
    return new Path(str).toUri().getPath();
  }

  /**
   * Constructor.
   * @param str a string representation of the parameter value.
   */
  public DestinationParam(final String str) {
    super(DOMAIN, validate(str));
  }

  @Override
  public String getName() {
    return NAME;
  }
}
