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

package org.apache.hadoop.io;

/**
 * Wrapper for {@link UTF8}.
 * This class should be used only when it is absolutely necessary
 * to use {@link UTF8}. The only difference is that using this class
 * does not require "@SupressWarning" annotation to avoid javac warning. 
 * In stead the deprecation is implied in the class name.
 */
public class DeprecatedUTF8 extends UTF8 {
  
  public DeprecatedUTF8() {
    super();
  }

  /** Construct from a given string. */
  public DeprecatedUTF8(String string) {
    super(string);
  }

  /** Construct from a given string. */
  public DeprecatedUTF8(DeprecatedUTF8 utf8) {
    super(utf8);
  }
}
