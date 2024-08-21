/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.hadoop.util.dynamic;

/**
 * This is a class for testing {@link DynMethods} and {@code DynConstructors}.
 * <p>
 * Derived from {@code org.apache.parquet.util} test suites.
 */
public class Concatenator {

  public static class SomeCheckedException extends Exception {
  }

  private String sep = "";

  public Concatenator() {
  }

  public Concatenator(String sep) {
    this.sep = sep;
  }

  private Concatenator(char sep) {
    this.sep = String.valueOf(sep);
  }

  public Concatenator(Exception e) throws Exception {
    throw e;
  }

  public static Concatenator newConcatenator(String sep) {
    return new Concatenator(sep);
  }

  private void setSeparator(String value) {
    this.sep = value;
  }

  public String concat(String left, String right) {
    return left + sep + right;
  }

  public String concat(String left, String middle, String right) {
    return left + sep + middle + sep + right;
  }

  public String concat(Exception e) throws Exception {
    throw e;
  }

  public String concat(String... strings) {
    if (strings.length >= 1) {
      StringBuilder sb = new StringBuilder();
      sb.append(strings[0]);
      for (int i = 1; i < strings.length; i += 1) {
        sb.append(sep);
        sb.append(strings[i]);
      }
      return sb.toString();
    }
    return null;
  }

  public static String cat(String... strings) {
    return new Concatenator().concat(strings);
  }
}
