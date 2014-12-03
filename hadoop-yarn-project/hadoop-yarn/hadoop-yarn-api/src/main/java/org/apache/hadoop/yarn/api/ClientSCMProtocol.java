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

package org.apache.hadoop.yarn.api;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.ReleaseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReleaseSharedCacheResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * <p>
 * The protocol between clients and the <code>SharedCacheManager</code> to claim
 * and release resources in the shared cache.
 * </p>
 */
@Public
@Unstable
public interface ClientSCMProtocol {
  /**
   * <p>
   * The interface used by clients to claim a resource with the
   * <code>SharedCacheManager.</code> The client uses a checksum to identify the
   * resource and an {@link ApplicationId} to identify which application will be
   * using the resource.
   * </p>
   *
   * <p>
   * The <code>SharedCacheManager</code> responds with whether or not the
   * resource exists in the cache. If the resource exists, a <code>Path</code>
   * to the resource in the shared cache is returned. If the resource does not
   * exist, the response is empty.
   * </p>
   *
   * @param request request to claim a resource in the shared cache
   * @return response indicating if the resource is already in the cache
   * @throws YarnException
   * @throws IOException
   */
  public UseSharedCacheResourceResponse use(
      UseSharedCacheResourceRequest request) throws YarnException, IOException;

  /**
   * <p>
   * The interface used by clients to release a resource with the
   * <code>SharedCacheManager.</code> This method is called once an application
   * is no longer using a claimed resource in the shared cache. The client uses
   * a checksum to identify the resource and an {@link ApplicationId} to
   * identify which application is releasing the resource.
   * </p>
   *
   * <p>
   * Note: This method is an optimization and the client is not required to call
   * it for correctness.
   * </p>
   *
   * <p>
   * Currently the <code>SharedCacheManager</code> sends an empty response.
   * </p>
   *
   * @param request request to release a resource in the shared cache
   * @return (empty) response on releasing the resource
   * @throws YarnException
   * @throws IOException
   */
  public ReleaseSharedCacheResourceResponse release(
      ReleaseSharedCacheResourceRequest request) throws YarnException, IOException;

}
