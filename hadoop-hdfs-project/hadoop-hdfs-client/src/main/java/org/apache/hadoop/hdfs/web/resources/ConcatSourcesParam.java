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

/** The concat source paths parameter. */
public class ConcatSourcesParam extends StringParam {
  /** Parameter name. */
  public static final String NAME = "sources";

  public static final String DEFAULT = "";

  private static final Domain DOMAIN = new Domain(NAME, null);

  private static String paths2String(Path[] paths) {
    if (paths == null || paths.length == 0) {
      return "";
    }
    final StringBuilder b = new StringBuilder(paths[0].toUri().getPath());
    for(int i = 1; i < paths.length; i++) {
      b.append(',').append(paths[i].toUri().getPath());
    }
    return b.toString();
  }

  /**
   * Constructor.
   * @param str a string representation of the parameter value.
   */
  public ConcatSourcesParam(String str) {
    super(DOMAIN, str);
  }

  public ConcatSourcesParam(Path[] paths) {
    this(paths2String(paths));
  }

  @Override
  public String getName() {
    return NAME;
  }

  /** @return the absolute path. */
  public final String[] getAbsolutePaths() {
    final String[] paths = getValue().split(",");
    return paths;
  }
}
