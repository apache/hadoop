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

package org.apache.hadoop.yarn.client.api;


import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.impl.SharedCacheClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * This is the client for YARN's shared cache.
 */
@Public
@Unstable
public abstract class SharedCacheClient extends AbstractService {

  @Public
  public static SharedCacheClient createSharedCacheClient() {
    SharedCacheClient client = new SharedCacheClientImpl();
    return client;
  }

  @Private
  public SharedCacheClient(String name) {
    super(name);
  }

  /**
   * <p>
   * The method to claim a resource with the <code>SharedCacheManager.</code>
   * The client uses a checksum to identify the resource and an
   * {@link ApplicationId} to identify which application will be using the
   * resource.
   * </p>
   * 
   * <p>
   * The <code>SharedCacheManager</code> responds with whether or not the
   * resource exists in the cache. If the resource exists, a <code>Path</code>
   * to the resource in the shared cache is returned. If the resource does not
   * exist, null is returned instead.
   * </p>
   * 
   * @param applicationId ApplicationId of the application using the resource
   * @param resourceKey the key (i.e. checksum) that identifies the resource
   * @return Path to the resource, or null if it does not exist
   */
  @Public
  @Unstable
  public abstract Path use(ApplicationId applicationId, String resourceKey)
      throws YarnException;

  /**
   * <p>
   * The method to release a resource with the <code>SharedCacheManager.</code>
   * This method is called once an application is no longer using a claimed
   * resource in the shared cache. The client uses a checksum to identify the
   * resource and an {@link ApplicationId} to identify which application is
   * releasing the resource.
   * </p>
   * 
   * <p>
   * Note: This method is an optimization and the client is not required to call
   * it for correctness.
   * </p>
   * 
   * @param applicationId ApplicationId of the application releasing the
   *          resource
   * @param resourceKey the key (i.e. checksum) that identifies the resource
   */
  @Public
  @Unstable
  public abstract void release(ApplicationId applicationId, String resourceKey)
      throws YarnException;

  /**
   * A convenience method to calculate the checksum of a specified file.
   * 
   * @param sourceFile A path to the input file
   * @return A hex string containing the checksum digest
   * @throws IOException
   */
  @Public
  @Unstable
  public abstract String getFileChecksum(Path sourceFile) throws IOException;
}
