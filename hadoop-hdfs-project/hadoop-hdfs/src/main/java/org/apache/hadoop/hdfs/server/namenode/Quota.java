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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.util.EnumCounters;

/** Quota types. */
public enum Quota {
  /** The namespace usage, i.e. the number of name objects. */
  NAMESPACE,
  /** The storage space usage in bytes including replication. */
  STORAGESPACE;

  /** Counters for quota counts. */
  public static class Counts extends EnumCounters<Quota> {
    /** @return a new counter with the given namespace and storagespace usages. */
    public static Counts newInstance(long namespace, long storagespace) {
      final Counts c = new Counts();
      c.set(NAMESPACE, namespace);
      c.set(STORAGESPACE, storagespace);
      return c;
    }

    public static Counts newInstance() {
      return newInstance(0, 0);
    }

    Counts() {
      super(Quota.class);
    }
  }

  /**
   * Is quota violated?
   * The quota is violated if quota is set and usage &gt; quota.
   */
  public static boolean isViolated(final long quota, final long usage) {
    return quota >= 0 && usage > quota;
  }

  /**
   * Is quota violated?
   * The quota is violated if quota is set, delta &gt; 0 and
   * usage + delta &gt; quota.
   */
  static boolean isViolated(final long quota, final long usage,
      final long delta) {
    return quota >= 0 && delta > 0 && usage > quota - delta;
  }
}