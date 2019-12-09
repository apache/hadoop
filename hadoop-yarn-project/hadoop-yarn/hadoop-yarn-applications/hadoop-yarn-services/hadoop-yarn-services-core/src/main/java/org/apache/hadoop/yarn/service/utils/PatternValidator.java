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
 */

package org.apache.hadoop.yarn.service.utils;

import java.util.regex.Pattern;

/**
 * Utility class to validate strings against a predefined pattern.
 */
public class PatternValidator {

  public static final String E_INVALID_NAME =
      "Invalid name %s does not match the pattern %s ";
  private final Pattern valid;
  private final String pattern;

  public PatternValidator(String pattern) {
    this.pattern = pattern;
    valid = Pattern.compile(pattern);
  }

  /**
   * Validate the name -restricting it to the set defined in 
   * @param name name to validate
   * @throws IllegalArgumentException if not a valid name
   */
  public void validate(String name) {
    if (!matches(name)) {
      throw new IllegalArgumentException(
          String.format(E_INVALID_NAME, name, pattern));
    }
  }

  /**
   * Query to see if the pattern matches
   * @param name name to validate
   * @return true if the string matches the pattern
   */
  public boolean matches(String name) {
    return valid.matcher(name).matches();
  }
}
