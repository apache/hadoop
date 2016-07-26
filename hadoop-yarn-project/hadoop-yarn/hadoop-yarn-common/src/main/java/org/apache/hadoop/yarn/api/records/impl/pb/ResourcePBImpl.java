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

package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.ResourceNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceInformationProto;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;

import java.util.*;

@Private
@Unstable
public class ResourcePBImpl extends Resource {

  private static final Log LOG = LogFactory.getLog(ResourcePBImpl.class);

  ResourceProto proto = ResourceProto.getDefaultInstance();
  ResourceProto.Builder builder = null;
  boolean viaProto = false;

  private Map<String, ResourceInformation> resources;


  // call via ProtoUtils.convertToProtoFormat(Resource)
  static ResourceProto getProto(Resource r) {
    final ResourcePBImpl pb;
    if (r instanceof ResourcePBImpl) {
      pb = (ResourcePBImpl)r;
    } else {
      pb = new ResourcePBImpl();
      pb.setMemorySize(r.getMemorySize());
      pb.setVirtualCores(r.getVirtualCores());
    }
    return pb.getProto();
  }

  public ResourcePBImpl() {
    builder = ResourceProto.newBuilder();
  }

  public ResourcePBImpl(ResourceProto proto) {
    this.proto = proto;
    viaProto = true;
    this.resources = null;
    initResources();
  }

  public ResourceProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  @SuppressWarnings("deprecation")
  public int getMemory() {
    return castToIntSafely(this.getMemorySize());
  }

  @Override
  public long getMemorySize() {
    // memory should always be present
    initResources();
    ResourceInformation ri =
        this.getResourceInformation(ResourceInformation.MEMORY_MB.getName());
    return UnitsConversionUtil
        .convert(ri.getUnits(), ResourceInformation.MEMORY_MB.getUnits(),
            ri.getValue());
  }

  @Override
  @SuppressWarnings("deprecation")
  public void setMemory(int memory) {
    setMemorySize(memory);
  }

  @Override
  public void setMemorySize(long memory) {
    setResourceInformation(ResourceInformation.MEMORY_MB.getName(),
        ResourceInformation.newInstance(ResourceInformation.MEMORY_MB.getName(),
            ResourceInformation.MEMORY_MB.getUnits(), memory));

  }

  @Override
  public int getVirtualCores() {
    // vcores should always be present
    initResources();
    return this.getResourceValue(ResourceInformation.VCORES.getName())
        .intValue();
  }

  @Override
  public void setVirtualCores(int vCores) {
    setResourceInformation(ResourceInformation.VCORES.getName(),
        ResourceInformation.newInstance(ResourceInformation.VCORES.getName(),
            ResourceInformation.VCORES.getUnits(), (long) vCores));
  }

  private void initResources() {
    if (this.resources != null) {
      return;
    }
    ResourceProtoOrBuilder p = viaProto ? proto : builder;
    initResourcesMap();
    for (ResourceInformationProto entry : p.getResourceValueMapList()) {
      ResourceTypes type =
          entry.hasType() ? ProtoUtils.convertFromProtoFormat(entry.getType()) :
              ResourceTypes.COUNTABLE;
      String units = entry.hasUnits() ? entry.getUnits() : "";
      Long value = entry.hasValue() ? entry.getValue() : 0L;
      ResourceInformation ri =
          ResourceInformation.newInstance(entry.getKey(), units, value, type);
      if (resources.containsKey(ri.getName())) {
        resources.get(ri.getName()).setResourceType(ri.getResourceType());
        resources.get(ri.getName()).setUnits(ri.getUnits());
        resources.get(ri.getName()).setValue(value);
      } else {
        LOG.warn("Got unknown resource type: " + ri.getName() + "; skipping");
      }
    }
    this.setMemorySize(p.getMemory());
    this.setVirtualCores(p.getVirtualCores());
  }

  @Override
  public void setResourceInformation(String resource,
      ResourceInformation resourceInformation) {
    maybeInitBuilder();
    if (resource == null || resourceInformation == null) {
      throw new IllegalArgumentException(
          "resource and/or resourceInformation cannot be null");
    }
    if (!resource.equals(resourceInformation.getName())) {
      resourceInformation.setName(resource);
    }
    initResources();
    resources.put(resource, resourceInformation);
  }

  @Override
  public void setResourceValue(String resource, Long value)
      throws ResourceNotFoundException {
    maybeInitBuilder();
    initResources();
    if (resource == null) {
      throw new IllegalArgumentException("resource type object cannot be null");
    }
    if (resources == null || (!resources.containsKey(resource))) {
      throw new ResourceNotFoundException(
          "Resource " + resource + " not found");
    }
    resources.get(resource).setValue(value);
  }

  @Override
  public Map<String, ResourceInformation> getResources() {
    initResources();
    return Collections.unmodifiableMap(this.resources);
  }

  @Override
  public ResourceInformation getResourceInformation(String resource) {
    initResources();
    if (this.resources.containsKey(resource)) {
      return this.resources.get(resource);
    }
    throw new ResourceNotFoundException("Could not find entry for " + resource);
  }

  @Override
  public Long getResourceValue(String resource) {
    initResources();
    if (this.resources.containsKey(resource)) {
      return this.resources.get(resource).getValue();
    }
    throw new ResourceNotFoundException("Could not find entry for " + resource);
  }

  private void initResourcesMap() {
    if (resources == null) {
      resources = new HashMap<>();
      Map<String, ResourceInformation> types = ResourceUtils.getResourceTypes();
      if (types == null) {
        throw new YarnRuntimeException(
            "Got null return value from ResourceUtils.getResourceTypes()");
      }
      for (Map.Entry<String, ResourceInformation> entry : types.entrySet()) {
        resources.put(entry.getKey(),
            ResourceInformation.newInstance(entry.getValue()));
      }
    }
  }

  synchronized private void mergeLocalToBuilder() {
    builder.clearResourceValueMap();
    if (resources != null && !resources.isEmpty()) {
      for (Map.Entry<String, ResourceInformation> entry :
          resources.entrySet()) {
        ResourceInformationProto.Builder e =
            ResourceInformationProto.newBuilder();
        e.setKey(entry.getKey());
        e.setUnits(entry.getValue().getUnits());
        e.setType(
            ProtoUtils.converToProtoFormat(entry.getValue().getResourceType()));
        e.setValue(entry.getValue().getValue());
        builder.addResourceValueMap(e);
      }
    }
    builder.setMemory(this.getMemorySize());
    builder.setVirtualCores(this.getVirtualCores());
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }
}  
