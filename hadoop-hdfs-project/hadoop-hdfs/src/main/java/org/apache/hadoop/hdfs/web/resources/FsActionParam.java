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

import org.apache.hadoop.fs.permission.FsAction;

import java.util.regex.Pattern;

/** {@link FsAction} Parameter */
public class FsActionParam extends StringParam {

  /** Parameter name. */
  public static final String NAME = "fsaction";

  /** Default parameter value. */
  public static final String DEFAULT = NULL;

  private static String FS_ACTION_PATTERN = "[rwx-]{3}";

  private static final Domain DOMAIN = new Domain(NAME,
      Pattern.compile(FS_ACTION_PATTERN));

  /**
   * Constructor.
   * @param str a string representation of the parameter value.
   */
  public FsActionParam(final String str) {
    super(DOMAIN, str == null || str.equals(DEFAULT)? null: str);
  }

  /**
   * Constructor.
   * @param value the parameter value.
   */
  public FsActionParam(final FsAction value) {
    super(DOMAIN, value == null? null: value.SYMBOL);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
