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

package org.apache.hadoop.ozone.container.common.utils;

import com.google.common.base.Preconditions;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.utils.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class to implement reference counting over instances handed by Container
 * Cache.
 * Enable DEBUG log below will enable us quickly locate the leaked reference
 * from caller stack. When JDK9 StackWalker is available, we can switch to
 * StackWalker instead of new Exception().printStackTrace().
 */
public class ReferenceCountedDB implements Closeable {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReferenceCountedDB.class);
  private final AtomicInteger referenceCount;
  private final MetadataStore store;
  private final String containerDBPath;

  public ReferenceCountedDB(MetadataStore store, String containerDBPath) {
    this.referenceCount = new AtomicInteger(0);
    this.store = store;
    this.containerDBPath = containerDBPath;
  }

  public long getReferenceCount() {
    return referenceCount.get();
  }

  public void incrementReference() {
    this.referenceCount.incrementAndGet();
    if (LOG.isTraceEnabled()) {
      LOG.trace("IncRef {} to refCnt {}, stackTrace: {}", containerDBPath,
          referenceCount.get(), ExceptionUtils.getStackTrace(new Throwable()));
    }
  }

  public void decrementReference() {
    int refCount = this.referenceCount.decrementAndGet();
    Preconditions.checkArgument(refCount >= 0, "refCount:", refCount);
    if (LOG.isTraceEnabled()) {
      LOG.trace("DecRef {} to refCnt {}, stackTrace: {}", containerDBPath,
          referenceCount.get(), ExceptionUtils.getStackTrace(new Throwable()));
    }
  }

  public boolean cleanup() {
    if (referenceCount.get() == 0 && store != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Close {} refCnt {}", containerDBPath,
            referenceCount.get());
      }
      try {
        store.close();
        return true;
      } catch (Exception e) {
        LOG.error("Error closing DB. Container: " + containerDBPath, e);
        return false;
      }
    } else {
      return false;
    }
  }

  public MetadataStore getStore() {
    return store;
  }

  public void close() {
    decrementReference();
  }
}