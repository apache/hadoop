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
package org.apache.hadoop.utils;

import com.google.common.base.Strings;
import org.apache.hadoop.hdfs.DFSUtil;

/**
 * An utility class to filter levelDB keys.
 */
public class LevelDBKeyFilters {

  /**
   * Interface for levelDB key filters.
   */
  public interface LevelDBKeyFilter {
    /**
     * Filter levelDB key with a certain condition.
     *
     * @param preKey     previous key.
     * @param currentKey current key.
     * @param nextKey    next key.
     * @return true if a certain condition satisfied, return false otherwise.
     */
    boolean filterKey(byte[] preKey, byte[] currentKey, byte[] nextKey);
  }

  /**
   * Utility class to filter key by a string prefix. This filter
   * assumes keys can be parsed to a string.
   */
  public static class KeyPrefixFilter implements LevelDBKeyFilter {

    private String keyPrefix = null;

    public KeyPrefixFilter(String keyPrefix) {
      this.keyPrefix = keyPrefix;
    }

    @Override public boolean filterKey(byte[] preKey, byte[] currentKey,
        byte[] nextKey) {
      if (Strings.isNullOrEmpty(keyPrefix)) {
        return true;
      } else {
        return currentKey != null &&
            DFSUtil.bytes2String(currentKey).startsWith(keyPrefix);
      }
    }
  }
}
