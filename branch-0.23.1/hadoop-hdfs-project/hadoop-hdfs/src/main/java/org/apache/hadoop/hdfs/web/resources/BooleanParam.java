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

/** Boolean parameter. */
abstract class BooleanParam extends Param<Boolean, BooleanParam.Domain> {
  static final String TRUE = "true";
  static final String FALSE = "false";

  BooleanParam(final Domain domain, final Boolean value) {
    super(domain, value);
  }

  /** The domain of the parameter. */
  static final class Domain extends Param.Domain<Boolean> {
    Domain(final String paramName) {
      super(paramName);
    }

    @Override
    public String getDomain() {
      return "<" + NULL + " | boolean>";
    }

    @Override
    Boolean parse(final String str) {
      if (TRUE.equalsIgnoreCase(str)) {
        return true;
      } else if (FALSE.equalsIgnoreCase(str)) {
        return false;
      }
      throw new IllegalArgumentException("Failed to parse \"" + str
          + "\" to Boolean.");
    }
  }
}
