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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ResourceEvent;

/**
 * A collection of {@link LocalizedResource}s all of same
 * {@link LocalResourceVisibility}.
 * 
 */

class LocalResourcesTrackerImpl implements LocalResourcesTracker {

  static final Log LOG = LogFactory.getLog(LocalResourcesTrackerImpl.class);

  private final String user;
  private final Dispatcher dispatcher;
  private final ConcurrentMap<LocalResourceRequest,LocalizedResource> localrsrc;

  public LocalResourcesTrackerImpl(String user, Dispatcher dispatcher) {
    this(user, dispatcher,
        new ConcurrentHashMap<LocalResourceRequest,LocalizedResource>());
  }

  LocalResourcesTrackerImpl(String user, Dispatcher dispatcher,
      ConcurrentMap<LocalResourceRequest,LocalizedResource> localrsrc) {
    this.user = user;
    this.dispatcher = dispatcher;
    this.localrsrc = localrsrc;
  }

  @Override
  public void handle(ResourceEvent event) {
    LocalResourceRequest req = event.getLocalResourceRequest();
    LocalizedResource rsrc = localrsrc.get(req);
    switch (event.getType()) {
    case REQUEST:
    case LOCALIZED:
      if (null == rsrc) {
        rsrc = new LocalizedResource(req, dispatcher);
        localrsrc.put(req, rsrc);
      }
      break;
    case RELEASE:
      if (null == rsrc) {
        LOG.info("Release unknown rsrc null (discard)");
        return;
      }
      break;
    }
    rsrc.handle(event);
  }

  @Override
  public boolean contains(LocalResourceRequest resource) {
    return localrsrc.containsKey(resource);
  }

  @Override
  public boolean remove(LocalizedResource rem, DeletionService delService) {
    // current synchronization guaranteed by crude RLS event for cleanup
    LocalizedResource rsrc = localrsrc.get(rem.getRequest());
    if (null == rsrc) {
      LOG.error("Attempt to remove absent resource: " + rem.getRequest() +
          " from " + getUser());
      return true;
    }
    if (rsrc.getRefCount() > 0
        || ResourceState.DOWNLOADING.equals(rsrc.getState())
        || rsrc != rem) {
      // internal error
      LOG.error("Attempt to remove resource: " + rsrc + " with non-zero refcount");
      assert false;
      return false;
    }
    localrsrc.remove(rem.getRequest());
    if (ResourceState.LOCALIZED.equals(rsrc.getState())) {
      delService.delete(getUser(), rsrc.getLocalPath());
    }
    return true;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public Iterator<LocalizedResource> iterator() {
    return localrsrc.values().iterator();
  }

}
