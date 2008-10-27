/**
 * Copyright 2008 The Apache Software Foundation
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
package org.apache.hadoop.hbase.util;

import java.lang.ref.ReferenceQueue;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * java.lang.ref.ReferenceQueue utility class.
 * @param <K>
 * @param <V>
 */
class ReferenceQueueUtil<K,V> {
  private final Log LOG = LogFactory.getLog(this.getClass());  
  private final ReferenceQueue rq = new ReferenceQueue();
  private final Map<K,V> map;

  private ReferenceQueueUtil() {
    this(null);
  }
  
  ReferenceQueueUtil(final Map<K,V> m) {
    super();
    this.map = m;
  }

  public ReferenceQueue getReferenceQueue() {
    return rq;
  }
  
  /**
   * Check the reference queue and delete anything that has since gone away
   */ 
  @SuppressWarnings("unchecked")
  void checkReferences() {
    int i = 0;
    for (Object obj = null; (obj = this.rq.poll()) != null;) {
      i++;
      this.map.remove(((SoftValue<K,V>)obj).getKey());
    }
    if (i > 0 && LOG.isDebugEnabled()) {
      LOG.debug("" + i + " reference(s) cleared.");
    }
  }
}