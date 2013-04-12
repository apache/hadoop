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
package org.apache.hadoop.tools.rumen.datatypes;

import org.apache.hadoop.conf.Configuration;

/**
 * Represents a class name.
 */
public class ClassName extends DefaultAnonymizableDataType {
  public static final String CLASSNAME_PRESERVE_CONFIG = "rumen.data-types.classname.preserve";
  private final String className;
  
  public ClassName(String className) {
    super();
    this.className = className;
  }
  
  @Override
  public String getValue() {
    return className;
  }
  
  @Override
  protected String getPrefix() {
    return "class";
  }
  
  @Override
  protected boolean needsAnonymization(Configuration conf) {
    String[] preserves = conf.getStrings(CLASSNAME_PRESERVE_CONFIG);
    if (preserves != null) {
      // do a simple starts with check
      for (String p : preserves) {
        if (className.startsWith(p)) {
          return false;
        }
      }
    }
    return true;
  }
}