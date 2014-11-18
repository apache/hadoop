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

package org.apache.hadoop.yarn.sharedcache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

@SuppressWarnings("unchecked")
@Public
@Evolving
/**
 * A factory class for creating checksum objects based on a configurable
 * algorithm implementation
 */
public class SharedCacheChecksumFactory {
  private static final
      ConcurrentMap<Class<? extends SharedCacheChecksum>,SharedCacheChecksum>
      instances =
          new ConcurrentHashMap<Class<? extends SharedCacheChecksum>,
          SharedCacheChecksum>();

  private static final Class<? extends SharedCacheChecksum> defaultAlgorithm;

  static {
    try {
      defaultAlgorithm = (Class<? extends SharedCacheChecksum>)
          Class.forName(
              YarnConfiguration.DEFAULT_SHARED_CACHE_CHECKSUM_ALGO_IMPL);
    } catch (Exception e) {
      // cannot happen
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Get a new <code>SharedCacheChecksum</code> object based on the configurable
   * algorithm implementation
   * (see <code>yarn.sharedcache.checksum.algo.impl</code>)
   *
   * @return <code>SharedCacheChecksum</code> object
   */
  public static SharedCacheChecksum getChecksum(Configuration conf) {
    Class<? extends SharedCacheChecksum> clazz =
        conf.getClass(YarnConfiguration.SHARED_CACHE_CHECKSUM_ALGO_IMPL,
        defaultAlgorithm, SharedCacheChecksum.class);
    SharedCacheChecksum checksum = instances.get(clazz);
    if (checksum == null) {
      try {
        checksum = ReflectionUtils.newInstance(clazz, conf);
        SharedCacheChecksum old = instances.putIfAbsent(clazz, checksum);
        if (old != null) {
          checksum = old;
        }
      } catch (Exception e) {
        throw new YarnRuntimeException(e);
      }
    }

    return checksum;
  }
}
