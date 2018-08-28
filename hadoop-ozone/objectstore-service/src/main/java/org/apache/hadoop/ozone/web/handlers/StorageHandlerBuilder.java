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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ozone.web.interfaces.StorageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class is responsible for providing a {@link StorageHandler}
 * implementation to object store web handlers.
 */
@InterfaceAudience.Private
public final class StorageHandlerBuilder {


  private static final Logger LOG =
      LoggerFactory.getLogger(StorageHandlerBuilder.class);
  private static final ThreadLocal<StorageHandler>
      STORAGE_HANDLER_THREAD_LOCAL = new ThreadLocal<>();

  /**
   * Returns the configured StorageHandler from thread-local storage for this
   * thread.
   *
   * @return StorageHandler from thread-local storage
   */
  public static StorageHandler getStorageHandler() throws IOException {
    StorageHandler storageHandler = STORAGE_HANDLER_THREAD_LOCAL.get();
    if (storageHandler != null) {
      return storageHandler;
    } else {
      LOG.error("No Storage Handler Configured.");
      throw new IOException("Invalid Handler Configuration");
    }

  }

  /**
   * Removes the configured StorageHandler from thread-local storage for this
   * thread.
   */
  public static void removeStorageHandler() {
    STORAGE_HANDLER_THREAD_LOCAL.remove();
  }

  /**
   * Sets the configured StorageHandler in thread-local storage for this thread.
   *
   * @param storageHandler StorageHandler to set in thread-local storage
   */
  public static void setStorageHandler(StorageHandler storageHandler) {
    STORAGE_HANDLER_THREAD_LOCAL.set(storageHandler);
  }

  /**
   * There is no reason to instantiate this class.
   */
  private StorageHandlerBuilder() {
  }
}
