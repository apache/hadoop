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
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * An utility class to filter levelDB keys.
 */
public final class MetadataKeyFilters {

  private static KeyPrefixFilter deletingKeyFilter =
      new MetadataKeyFilters.KeyPrefixFilter(OzoneConsts.DELETING_KEY_PREFIX);

  private static KeyPrefixFilter normalKeyFilter =
      new MetadataKeyFilters.KeyPrefixFilter(OzoneConsts.DELETING_KEY_PREFIX,
          true);

  private MetadataKeyFilters() {
  }

  public static KeyPrefixFilter getDeletingKeyFilter() {
    return deletingKeyFilter;
  }

  public static KeyPrefixFilter getNormalKeyFilter() {
    return normalKeyFilter;
  }
  /**
   * Interface for levelDB key filters.
   */
  public interface MetadataKeyFilter {
    /**
     * Filter levelDB key with a certain condition.
     *
     * @param preKey     previous key.
     * @param currentKey current key.
     * @param nextKey    next key.
     * @return true if a certain condition satisfied, return false otherwise.
     */
    boolean filterKey(byte[] preKey, byte[] currentKey, byte[] nextKey);

    default int getKeysScannedNum() {
      return 0;
    }

    default int getKeysHintedNum() {
      return 0;
    }
  }

  /**
   * Utility class to filter key by a string prefix. This filter
   * assumes keys can be parsed to a string.
   */
  public static class KeyPrefixFilter implements MetadataKeyFilter {

    private String keyPrefix = null;
    private int keysScanned = 0;
    private int keysHinted = 0;
    private Boolean negative;

    public KeyPrefixFilter(String keyPrefix) {
      this(keyPrefix, false);
    }

    public KeyPrefixFilter(String keyPrefix, boolean negative) {
      this.keyPrefix = keyPrefix;
      this.negative = negative;
    }

    @Override
    public boolean filterKey(byte[] preKey, byte[] currentKey,
        byte[] nextKey) {
      keysScanned++;
      boolean accept = false;
      if (Strings.isNullOrEmpty(keyPrefix)) {
        accept = true;
      } else {
        if (currentKey != null &&
            DFSUtil.bytes2String(currentKey).startsWith(keyPrefix)) {
          keysHinted++;
          accept = true;
        } else {
          accept = false;
        }
      }
      return (negative) ? !accept : accept;
    }

    @Override
    public int getKeysScannedNum() {
      return keysScanned;
    }

    @Override
    public int getKeysHintedNum() {
      return keysHinted;
    }
  }
}
