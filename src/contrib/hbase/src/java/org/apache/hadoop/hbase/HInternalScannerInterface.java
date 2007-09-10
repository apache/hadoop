/**
 * Copyright 2007 The Apache Software Foundation
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

/**
 * Internally, we need to be able to determine if the scanner is doing wildcard
 * column matches (when only a column family is specified or if a column regex
 * is specified) or if multiple members of the same column family were
 * specified. If so, we need to ignore the timestamp to ensure that we get all
 * the family members, as they may have been last updated at different times.
 */
public interface HInternalScannerInterface extends HScannerInterface {
  /** @return true if the scanner is matching a column family or regex */
  public boolean isWildcardScanner();
  
  /** @return true if the scanner is matching multiple column family members */
  public boolean isMultipleMatchScanner();
}