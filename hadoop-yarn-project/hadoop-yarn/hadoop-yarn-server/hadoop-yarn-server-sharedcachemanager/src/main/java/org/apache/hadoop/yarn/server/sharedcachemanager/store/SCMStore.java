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

package org.apache.hadoop.yarn.server.sharedcachemanager.store;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.sharedcachemanager.AppChecker;

import com.google.common.annotations.VisibleForTesting;


/**
 * An abstract class for the data store used by the shared cache manager
 * service. All implementations of methods in this interface need to be thread
 * safe and atomic.
 */
@Private
@Evolving
public abstract class SCMStore extends CompositeService {

  protected AppChecker appChecker;

  protected SCMStore(String name) {
    super(name);
  }

  @VisibleForTesting
  SCMStore(String name, AppChecker appChecker) {
    super(name);
    this.appChecker = appChecker;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    if (this.appChecker == null) {
      this.appChecker = createAppCheckerService(conf);
    }
    addService(appChecker);
    super.serviceInit(conf);
  }

  /**
   * Add a resource to the shared cache and it's associated filename. The
   * resource is identified by a unique key. If the key already exists no action
   * is taken and the filename of the existing resource is returned. If the key
   * does not exist, the resource is added, it's access time is set, and the
   * filename of the resource is returned.
   * 
   * @param key a unique identifier for a resource
   * @param fileName the filename of the resource
   * @return the filename of the resource as represented by the cache
   */
  @Private
  public abstract String addResource(String key, String fileName);

  /**
   * Remove a resource from the shared cache.
   * 
   * @param key a unique identifier for a resource
   * @return true if the resource was removed or did not exist, false if the
   *         resource existed, contained at least one
   *         <code>SharedCacheResourceReference</code> and was not removed.
   */
  @Private
  public abstract boolean removeResource(String key);

  /**
   * Add a <code>SharedCacheResourceReference</code> to a resource and update
   * the resource access time.
   * 
   * @param key a unique identifier for a resource
   * @param ref the <code>SharedCacheResourceReference</code> to add
   * @return String the filename of the resource if the
   *         <code>SharedCacheResourceReference</code> was added or already
   *         existed. null if the resource did not exist
   */
  @Private
  public abstract String addResourceReference(String key,
      SharedCacheResourceReference ref);

  /**
   * Get the <code>SharedCacheResourceReference</code>(s) associated with the
   * resource.
   * 
   * @param key a unique identifier for a resource
   * @return an unmodifiable collection of
   *         <code>SharedCacheResourceReferences</code>. If the resource does
   *         not exist, an empty set is returned.
   */
  @Private
  public abstract Collection<SharedCacheResourceReference> getResourceReferences(
      String key);

  /**
   * Remove a <code>SharedCacheResourceReference</code> from a resource.
   * 
   * @param key a unique identifier for a resource
   * @param ref the <code>SharedCacheResourceReference</code> to remove
   * @param updateAccessTime true if the call should update the access time for
   *          the resource
   * @return true if the reference was removed, false otherwise
   */
  @Private
  public abstract boolean removeResourceReference(String key,
      SharedCacheResourceReference ref, boolean updateAccessTime);

  /**
   * Remove a collection of <code>SharedCacheResourceReferences</code> from a
   * resource.
   * 
   * @param key a unique identifier for a resource
   * @param refs the collection of <code>SharedCacheResourceReference</code>s to
   *          remove
   * @param updateAccessTime true if the call should update the access time for
   *          the resource
   */
  @Private
  public abstract void removeResourceReferences(String key,
      Collection<SharedCacheResourceReference> refs, boolean updateAccessTime);

  /**
   * Clean all resource references to a cache resource that contain application
   * ids pointing to finished applications. If the resource key does not exist,
   * do nothing.
   *
   * @param key a unique identifier for a resource
   * @throws YarnException
   */
  @Private
  public void cleanResourceReferences(String key) throws YarnException {
    Collection<SharedCacheResourceReference> refs = getResourceReferences(key);
    if (!refs.isEmpty()) {
      Set<SharedCacheResourceReference> refsToRemove =
          new HashSet<SharedCacheResourceReference>();
      for (SharedCacheResourceReference r : refs) {
        if (!appChecker.isApplicationActive(r.getAppId())) {
          // application in resource reference is dead, it is safe to remove the
          // reference
          refsToRemove.add(r);
        }
      }
      if (refsToRemove.size() > 0) {
        removeResourceReferences(key, refsToRemove, false);
      }
    }
  }

  /**
   * Check if a specific resource is evictable according to the store's enabled
   * cache eviction policies.
   * 
   * @param key a unique identifier for a resource
   * @param file the <code>FileStatus</code> object for the resource file in the
   *          file system.
   * @return true if the resource is evicatble, false otherwise
   */
  @Private
  public abstract boolean isResourceEvictable(String key, FileStatus file);

  /**
   * Create an instance of the AppChecker service via reflection based on the
   * {@link YarnConfiguration#SCM_APP_CHECKER_CLASS} parameter.
   * 
   * @param conf
   * @return an instance of the AppChecker class
   */
  @Private
  @SuppressWarnings("unchecked")
  public static AppChecker createAppCheckerService(Configuration conf) {
    Class<? extends AppChecker> defaultCheckerClass;
    try {
      defaultCheckerClass =
          (Class<? extends AppChecker>) Class
              .forName(YarnConfiguration.DEFAULT_SCM_APP_CHECKER_CLASS);
    } catch (Exception e) {
      throw new YarnRuntimeException("Invalid default scm app checker class"
          + YarnConfiguration.DEFAULT_SCM_APP_CHECKER_CLASS, e);
    }

    AppChecker checker =
        ReflectionUtils.newInstance(conf.getClass(
            YarnConfiguration.SCM_APP_CHECKER_CLASS, defaultCheckerClass,
            AppChecker.class), conf);
    return checker;
  }
}
