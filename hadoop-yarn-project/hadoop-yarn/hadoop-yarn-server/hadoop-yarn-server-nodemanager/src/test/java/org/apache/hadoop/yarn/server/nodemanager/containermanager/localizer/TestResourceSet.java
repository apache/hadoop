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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.LocalizationState;
import org.apache.hadoop.yarn.api.records.LocalizationStatus;
import org.apache.hadoop.yarn.api.records.URL;
import org.junit.Assert;
import org.junit.Test;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests of {@link ResourceSet}.
 */
public class TestResourceSet {

  @Test
  public void testGetPendingLS() throws URISyntaxException {
    ResourceSet resourceSet = new ResourceSet();
    Map<String, LocalResource> resources = new HashMap<>();
    resources.put("resource1",
        LocalResource.newInstance(URL.fromPath(new Path("/tmp/file1.txt")),
            LocalResourceType.FILE, LocalResourceVisibility.PRIVATE,
            0, System.currentTimeMillis()));
    resourceSet.addResources(resources);

    Assert.assertEquals("num statuses", 1,
        resourceSet.getLocalizationStatuses().size());
    LocalizationStatus status = resourceSet.getLocalizationStatuses()
        .iterator().next();
    Assert.assertEquals("status", LocalizationState.PENDING,
        status.getLocalizationState());
  }

  @Test
  public void testGetCompletedLS() throws URISyntaxException {
    ResourceSet resourceSet = new ResourceSet();
    Map<String, LocalResource> resources = new HashMap<>();
    LocalResource resource1 = LocalResource.newInstance(
        URL.fromPath(new Path("/tmp/file1.txt")),
        LocalResourceType.FILE, LocalResourceVisibility.PRIVATE,
        0, System.currentTimeMillis());

    resources.put("resource1", resource1);
    resourceSet.addResources(resources);

    LocalResourceRequest lrr = new LocalResourceRequest(resource1);
    resourceSet.resourceLocalized(lrr, new Path("file1.txt"));

    Assert.assertEquals("num statuses", 1,
        resourceSet.getLocalizationStatuses().size());
    LocalizationStatus status = resourceSet.getLocalizationStatuses()
        .iterator().next();
    Assert.assertEquals("status", LocalizationState.COMPLETED,
        status.getLocalizationState());
  }


  @Test
  public void testGetFailedLS() throws URISyntaxException {
    ResourceSet resourceSet = new ResourceSet();
    Map<String, LocalResource> resources = new HashMap<>();
    LocalResource resource1 = LocalResource.newInstance(
        URL.fromPath(new Path("/tmp/file1.txt")),
        LocalResourceType.FILE, LocalResourceVisibility.PRIVATE,
        0, System.currentTimeMillis());

    resources.put("resource1", resource1);
    resourceSet.addResources(resources);

    LocalResourceRequest lrr = new LocalResourceRequest(resource1);
    resourceSet.resourceLocalizationFailed(lrr, "file does not exist");

    Assert.assertEquals("num statuses", 1,
        resourceSet.getLocalizationStatuses().size());
    LocalizationStatus status = resourceSet.getLocalizationStatuses()
        .iterator().next();
    Assert.assertEquals("status", LocalizationState.FAILED,
        status.getLocalizationState());
    Assert.assertEquals("diagnostics", "file does not exist",
        status.getDiagnostics());
  }
}
