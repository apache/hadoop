/*
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

package org.apache.slider.server.appmaster.web.rest.application.resources;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache of content
 */
public class ContentCache extends ConcurrentHashMap<String, CachedContent> {

  public ContentCache(int initialCapacity) {
    super(initialCapacity);
  }

  public ContentCache() {
  }


  public Object lookup(String key) throws Exception {
    CachedContent content = get(key);
    if (content == null) {
      throw new FileNotFoundException("no content for path " + key);
    }
    return content.get();
  }


  /**
   * Lookup a cached item. If an exception is raised on the refresh...
   * <ol>
   *   <li>IOExceptions are thrown directly</li>
   *   <li>Other exceptions are wrapped with an IOExceptions</li>
   * </ol>
   * @param key
   * @return
   * @throws IOException
   */
  public Object lookupWithIOE(String key) throws IOException {
    try {
      return lookup(key);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Looking up " + key + ": " + e, e);
    }
  }

}
