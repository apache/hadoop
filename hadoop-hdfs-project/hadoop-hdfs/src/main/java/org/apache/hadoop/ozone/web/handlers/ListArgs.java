/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.web.handlers;

/**
 * Supports listing keys with pagination.
 */
public class ListArgs extends BucketArgs {
  private String startPage;
  private String prefix;
  private int maxKeys;

  /**
   * Constructor for ListArgs.
   *
   * @param args      - BucketArgs
   * @param prefix    Prefix to start Query from
   * @param maxKeys   Max result set
   * @param startPage - Page token
   */
  public ListArgs(BucketArgs args, String prefix, int maxKeys,
                  String startPage) {
    super(args);
    setPrefix(prefix);
    setMaxKeys(maxKeys);
    setStartPage(startPage);
  }

  /**
   * Copy Constructor for ListArgs.
   *
   * @param args - List Args
   */
  public ListArgs(ListArgs args) {
    this(args, args.getPrefix(), args.getMaxKeys(), args.getStartPage());
  }

  /**
   * Returns page token.
   *
   * @return String
   */
  public String getStartPage() {
    return startPage;
  }

  /**
   * Sets page token.
   *
   * @param startPage - Page token
   */
  public void setStartPage(String startPage) {
    this.startPage = startPage;
  }

  /**
   * Gets max keys.
   *
   * @return int
   */
  public int getMaxKeys() {
    return maxKeys;
  }

  /**
   * Sets max keys.
   *
   * @param maxKeys - Maximum keys to return
   */
  public void setMaxKeys(int maxKeys) {
    this.maxKeys = maxKeys;
  }

  /**
   * Gets prefix.
   *
   * @return String
   */
  public String getPrefix() {
    return prefix;
  }

  /**
   * Sets prefix.
   *
   * @param prefix - The prefix that we are looking for
   */
  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }
}