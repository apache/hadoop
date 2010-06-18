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

package org.apache.hadoop.hdfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A simple wrapper around {@link org.apache.hadoop.io.UTF8}.
 * This class should be used only when it is absolutely necessary
 * to use {@link org.apache.hadoop.io.UTF8}. The only difference is that 
 * using this class does not require "@SuppressWarning" annotation to avoid 
 * javac warning. Instead the deprecation is implied in the class name.
 * 
 * This should be treated as package private class to HDFS.
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class DeprecatedUTF8 extends org.apache.hadoop.io.UTF8 {
  
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
  
  /* The following two are the mostly commonly used methods.
   * wrapping them so that editors do not complain about the deprecation.
   */
  
  public static String readString(DataInput in) throws IOException {
    return org.apache.hadoop.io.UTF8.readString(in);
  }
  
  public static int writeString(DataOutput out, String s) throws IOException {
    return org.apache.hadoop.io.UTF8.writeString(out, s);
  }
}
