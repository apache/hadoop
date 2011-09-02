/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Thrown if a table should be enabled but is not
 */
public class TableNotEnabledException extends IOException {
  private static final long serialVersionUID = 262144L;
  /** default constructor */
  public TableNotEnabledException() {
    super();
  }

  /**
   * Constructor
   * @param s message
   */
  public TableNotEnabledException(String s) {
    super(s);
  }

  /**
   * @param tableName Name of table that is not enabled
   */
  public TableNotEnabledException(byte[] tableName) {
    this(Bytes.toString(tableName));
  }
}