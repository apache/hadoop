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
public class ListArgs<T extends UserArgs> {
  private String prevKey;
  private String prefix;
  private int maxKeys;
  private boolean rootScan;
  private T args;

  /**
   * Constructor for ListArgs.
   *
   * @param args      - BucketArgs
   * @param prefix    Prefix to start Query from
   * @param maxKeys   Max result set
   * @param prevKey - Page token
   */
  public ListArgs(T args, String prefix, int maxKeys,
                  String prevKey) {
    setArgs(args);
    setPrefix(prefix);
    setMaxKeys(maxKeys);
    setPrevKey(prevKey);
  }

  /**
   * Copy Constructor for ListArgs.
   *
   * @param args - List Args
   */
  public ListArgs(T args, ListArgs listArgs) {
    this(args, listArgs.getPrefix(), listArgs.getMaxKeys(),
        listArgs.getPrevKey());
  }

  /**
   * Returns page token.
   *
   * @return String
   */
  public String getPrevKey() {
    return prevKey;
  }

  /**
   * Sets page token.
   *
   * @param prevKey - Page token
   */
  public void setPrevKey(String prevKey) {
    this.prevKey = prevKey;
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

  /**
   * Gets args.
   * @return  T
   */
  public T getArgs() {
    return args;
  }

  /**
   * Sets  args.
   * @param args T
   */
  public void setArgs(T args) {
    this.args = args;
  }

  /**
   * Checks if we are doing a rootScan.
   * @return - RootScan.
   */
  public boolean isRootScan() {
    return rootScan;
  }

  /**
   * Sets the RootScan property.
   * @param rootScan - Boolean.
   */
  public void setRootScan(boolean rootScan) {
    this.rootScan = rootScan;
  }

}