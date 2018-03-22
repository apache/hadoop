/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * All Resources requested by the container.
 */
public class ResourceSet {

  private static final Logger LOG =
       LoggerFactory.getLogger(ResourceSet.class);

  // resources by localization state (localized, pending, failed)
  private Map<String, Path> localizedResources =
      new ConcurrentHashMap<>();
  private Map<LocalResourceRequest, Set<String>> pendingResources =
      new ConcurrentHashMap<>();
  private Set<LocalResourceRequest> resourcesFailedToBeLocalized =
      new HashSet<>();

  // resources by visibility (public, private, app)
  private final List<LocalResourceRequest> publicRsrcs =
      new ArrayList<>();
  private final List<LocalResourceRequest> privateRsrcs =
      new ArrayList<>();
  private final List<LocalResourceRequest> appRsrcs =
      new ArrayList<>();

  private final Map<LocalResourceRequest, Path> resourcesToBeUploaded =
      new ConcurrentHashMap<>();
  private final Map<LocalResourceRequest, Boolean> resourcesUploadPolicies =
      new ConcurrentHashMap<>();

  public Map<LocalResourceVisibility, Collection<LocalResourceRequest>>
      addResources(Map<String, LocalResource> localResourceMap)
      throws URISyntaxException {
    if (localResourceMap == null || localResourceMap.isEmpty()) {
      return null;
    }
    Map<LocalResourceRequest, Set<String>> allResources = new HashMap<>();
    List<LocalResourceRequest> publicList = new ArrayList<>();
    List<LocalResourceRequest> privateList = new ArrayList<>();
    List<LocalResourceRequest> appList = new ArrayList<>();

    for (Map.Entry<String, LocalResource> rsrc : localResourceMap.entrySet()) {
      LocalResource resource = rsrc.getValue();
      LocalResourceRequest req = new LocalResourceRequest(rsrc.getValue());
      allResources.putIfAbsent(req, new HashSet<>());
      allResources.get(req).add(rsrc.getKey());
      storeSharedCacheUploadPolicy(req,
          resource.getShouldBeUploadedToSharedCache());
      switch (resource.getVisibility()) {
      case PUBLIC:
        publicList.add(req);
        break;
      case PRIVATE:
        privateList.add(req);
        break;
      case APPLICATION:
        appList.add(req);
        break;
      default:
        break;
      }
    }
    Map<LocalResourceVisibility, Collection<LocalResourceRequest>> req =
        new LinkedHashMap<>();
    if (!publicList.isEmpty()) {
      publicRsrcs.addAll(publicList);
      req.put(LocalResourceVisibility.PUBLIC, publicList);
    }
    if (!privateList.isEmpty()) {
      privateRsrcs.addAll(privateList);
      req.put(LocalResourceVisibility.PRIVATE, privateList);
    }
    if (!appList.isEmpty()) {
      appRsrcs.addAll(appList);
      req.put(LocalResourceVisibility.APPLICATION, appList);
    }
    if (!allResources.isEmpty()) {
      this.pendingResources.putAll(allResources);
    }
    return req;
  }

  /**
   * Called when resource localized.
   * @param request The original request for the localized resource
   * @param location The path where the resource is localized
   * @return The list of symlinks for the localized resources.
   */
  public Set<String> resourceLocalized(LocalResourceRequest request,
      Path location) {
    Set<String> symlinks = pendingResources.remove(request);
    if (symlinks == null) {
      return null;
    } else {
      for (String symlink : symlinks) {
        localizedResources.put(symlink, location);
      }
      return symlinks;
    }
  }

  public void resourceLocalizationFailed(LocalResourceRequest request) {
    // Skip null request when localization failed for running container
    if (request == null) {
      return;
    }
    pendingResources.remove(request);
    resourcesFailedToBeLocalized.add(request);
  }

  public synchronized Map<LocalResourceVisibility,
      Collection<LocalResourceRequest>> getAllResourcesByVisibility() {

    Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrc =
        new HashMap<>();
    if (!publicRsrcs.isEmpty()) {
      rsrc.put(LocalResourceVisibility.PUBLIC, publicRsrcs);
    }
    if (!privateRsrcs.isEmpty()) {
      rsrc.put(LocalResourceVisibility.PRIVATE, privateRsrcs);
    }
    if (!appRsrcs.isEmpty()) {
      rsrc.put(LocalResourceVisibility.APPLICATION, appRsrcs);
    }
    return rsrc;
  }

  /**
   * Store the resource's shared cache upload policies
   * Given LocalResourceRequest can be shared across containers in
   * LocalResourcesTrackerImpl, we preserve the upload policies here.
   * In addition, it is possible for the application to create several
   * "identical" LocalResources as part of
   * ContainerLaunchContext.setLocalResources with different symlinks.
   * There is a corner case where these "identical" local resources have
   * different upload policies. For that scenario, upload policy will be set to
   * true as long as there is at least one LocalResource entry with
   * upload policy set to true.
   */
  private void storeSharedCacheUploadPolicy(
      LocalResourceRequest resourceRequest, Boolean uploadPolicy) {
    Boolean storedUploadPolicy = resourcesUploadPolicies.get(resourceRequest);
    if (storedUploadPolicy == null || (!storedUploadPolicy && uploadPolicy)) {
      resourcesUploadPolicies.put(resourceRequest, uploadPolicy);
    }
  }

  public Map<Path, List<String>> getLocalizedResources() {
    Map<Path, List<String>> map = new HashMap<>();
    for (Map.Entry<String, Path> entry : localizedResources.entrySet()) {
      map.putIfAbsent(entry.getValue(), new ArrayList<>());
      map.get(entry.getValue()).add(entry.getKey());
    }
    return map;
  }

  public Map<LocalResourceRequest, Path> getResourcesToBeUploaded() {
    return resourcesToBeUploaded;
  }

  public Map<LocalResourceRequest, Boolean> getResourcesUploadPolicies() {
    return resourcesUploadPolicies;
  }

  public Map<LocalResourceRequest, Set<String>> getPendingResources() {
    return pendingResources;
  }

  public static ResourceSet merge(ResourceSet... resourceSets) {
    ResourceSet merged = new ResourceSet();
    for (ResourceSet rs : resourceSets) {
      // This should overwrite existing symlinks
      merged.localizedResources.putAll(rs.localizedResources);

      merged.resourcesToBeUploaded.putAll(rs.resourcesToBeUploaded);
      merged.resourcesUploadPolicies.putAll(rs.resourcesUploadPolicies);

      // TODO : START : Should we de-dup here ?
      merged.publicRsrcs.addAll(rs.publicRsrcs);
      merged.privateRsrcs.addAll(rs.privateRsrcs);
      merged.appRsrcs.addAll(rs.appRsrcs);
      // TODO : END
    }
    return merged;
  }
}
