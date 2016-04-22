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

package org.apache.hadoop.metrics2.lib;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Helpers to create interned metrics info.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Interns {
  private static final Logger LOG = LoggerFactory.getLogger(Interns.class);

  // A simple intern cache with two keys
  // (to avoid creating new (combined) key objects for lookup)
  private static abstract class CacheWith2Keys<K1, K2, V> {
    private final Map<K1, Map<K2, V>> k1Map =
        new LinkedHashMap<K1, Map<K2, V>>() {
      private static final long serialVersionUID = 1L;
      private boolean gotOverflow = false;
      @Override
      protected boolean removeEldestEntry(Map.Entry<K1, Map<K2, V>> e) {
        boolean overflow = expireKey1At(size());
        if (overflow && !gotOverflow) {
          LOG.info("Metrics intern cache overflow at {} for {}", size(), e);
          gotOverflow = true;
        }
        return overflow;
      }
    };

    abstract protected boolean expireKey1At(int size);
    abstract protected boolean expireKey2At(int size);
    abstract protected V newValue(K1 k1, K2 k2);

    synchronized V add(K1 k1, K2 k2) {
      Map<K2, V> k2Map = k1Map.get(k1);
      if (k2Map == null) {
        k2Map = new LinkedHashMap<K2, V>() {
          private static final long serialVersionUID = 1L;
          private boolean gotOverflow = false;
          @Override protected boolean removeEldestEntry(Map.Entry<K2, V> e) {
            boolean overflow = expireKey2At(size());
            if (overflow && !gotOverflow) {
              LOG.info("Metrics intern cache overflow at {} for {}", size(), e);
              gotOverflow = true;
            }
            return overflow;
          }
        };
        k1Map.put(k1, k2Map);
      }
      V v = k2Map.get(k2);
      if (v == null) {
        v = newValue(k1, k2);
        k2Map.put(k2, v);
      }
      return v;
    }
  }

  // Sanity limits in case of misuse/abuse.
  static final int MAX_INFO_NAMES = 2010;
  static final int MAX_INFO_DESCS = 100;  // distinct per name

  enum Info {
    INSTANCE;

    final CacheWith2Keys<String, String, MetricsInfo> cache =
        new CacheWith2Keys<String, String, MetricsInfo>() {

      @Override protected boolean expireKey1At(int size) {
        return size > MAX_INFO_NAMES;
      }

      @Override protected boolean expireKey2At(int size) {
        return size > MAX_INFO_DESCS;
      }

      @Override protected MetricsInfo newValue(String name, String desc) {
        return new MetricsInfoImpl(name, desc);
      }
    };
  }

  /**
   * Get a metric info object.
   * @param name Name of metric info object
   * @param description Description of metric info object
   * @return an interned metric info object
   */
  public static MetricsInfo info(String name, String description) {
    return Info.INSTANCE.cache.add(name, description);
  }

  // Sanity limits
  static final int MAX_TAG_NAMES = 100;
  static final int MAX_TAG_VALUES = 1000; // distinct per name

  enum Tags {
    INSTANCE;

    final CacheWith2Keys<MetricsInfo, String, MetricsTag> cache =
        new CacheWith2Keys<MetricsInfo, String, MetricsTag>() {

      @Override protected boolean expireKey1At(int size) {
        return size > MAX_TAG_NAMES;
      }

      @Override protected boolean expireKey2At(int size) {
        return size > MAX_TAG_VALUES;
      }

      @Override protected MetricsTag newValue(MetricsInfo info, String value) {
        return new MetricsTag(info, value);
      }
    };
  }

  /**
   * Get a metrics tag.
   * @param info  of the tag
   * @param value of the tag
   * @return an interned metrics tag
   */
  public static MetricsTag tag(MetricsInfo info, String value) {
    return Tags.INSTANCE.cache.add(info, value);
  }

  /**
   * Get a metrics tag.
   * @param name  of the tag
   * @param description of the tag
   * @param value of the tag
   * @return an interned metrics tag
   */
  public static MetricsTag tag(String name, String description, String value) {
    return Tags.INSTANCE.cache.add(info(name, description), value);
  }
}
