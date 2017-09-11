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
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;

import java.util.Map;


@Private
@Unstable
public class ResourcePBImpl extends Resource {

  private static final Log LOG = LogFactory.getLog(ResourcePBImpl.class);

  ResourceProto proto = ResourceProto.getDefaultInstance();
  ResourceProto.Builder builder = null;
  boolean viaProto = false;

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
    initResources();
  }

  public ResourcePBImpl(ResourceProto proto) {
    this.proto = proto;
    viaProto = true;
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
    ResourceInformation ri = resources[MEMORY_INDEX];

    if (ri.getUnits().isEmpty()) {
      return ri.getValue();
    }
    return UnitsConversionUtil.convert(ri.getUnits(),
        ResourceInformation.MEMORY_MB.getUnits(), ri.getValue());
  }

  @Override
  @SuppressWarnings("deprecation")
  public void setMemory(int memory) {
    setMemorySize(memory);
  }

  @Override
  public void setMemorySize(long memory) {
    maybeInitBuilder();
    getResourceInformation(ResourceInformation.MEMORY_URI).setValue(memory);
  }

  @Override
  public int getVirtualCores() {
    // vcores should always be present
    return (int) resources[VCORES_INDEX].getValue();
  }

  @Override
  public void setVirtualCores(int vCores) {
    maybeInitBuilder();
    getResourceInformation(ResourceInformation.VCORES_URI).setValue(vCores);
  }

  private void initResources() {
    if (this.resources != null) {
      return;
    }
    ResourceProtoOrBuilder p = viaProto ? proto : builder;
    initResourcesMap();
    Map<String, Integer> indexMap = ResourceUtils.getResourceTypeIndex();
    for (ResourceInformationProto entry : p.getResourceValueMapList()) {
      ResourceTypes type =
          entry.hasType() ? ProtoUtils.convertFromProtoFormat(entry.getType()) :
              ResourceTypes.COUNTABLE;

      // When unit not specified in proto, use the default unit.
      String units =
          entry.hasUnits() ? entry.getUnits() : ResourceUtils.getDefaultUnit(
              entry.getKey());
      long value = entry.hasValue() ? entry.getValue() : 0L;
      ResourceInformation ri = ResourceInformation
          .newInstance(entry.getKey(), units, value, type, 0L, Long.MAX_VALUE);
      Integer index = indexMap.get(entry.getKey());
      if (index == null) {
        LOG.warn("Got unknown resource type: " + ri.getName() + "; skipping");
      } else {
        resources[index].setResourceType(ri.getResourceType());
        resources[index].setUnits(ri.getUnits());
        resources[index].setValue(value);
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
    ResourceInformation storedResourceInfo = getResourceInformation(resource);
    ResourceInformation.copy(resourceInformation, storedResourceInfo);
  }

  @Override
  public void setResourceValue(String resource, long value)
      throws ResourceNotFoundException {
    maybeInitBuilder();
    if (resource == null) {
      throw new IllegalArgumentException("resource type object cannot be null");
    }
    getResourceInformation(resource).setValue(value);
  }

  @Override
  public ResourceInformation getResourceInformation(String resource)
      throws ResourceNotFoundException {
    return super.getResourceInformation(resource);
  }

  @Override
  public long getResourceValue(String resource)
      throws ResourceNotFoundException {
    return super.getResourceValue(resource);
  }

  private void initResourcesMap() {
    if (resources == null) {
      ResourceInformation[] types = ResourceUtils.getResourceTypesArray();
      if (types == null) {
        throw new YarnRuntimeException(
            "Got null return value from ResourceUtils.getResourceTypes()");
      }

      resources = new ResourceInformation[types.length];
      for (ResourceInformation entry : types) {
        int index = ResourceUtils.getResourceTypeIndex().get(entry.getName());
        resources[index] = ResourceInformation.newInstance(entry);
      }
    }
  }

  synchronized private void mergeLocalToBuilder() {
    builder.clearResourceValueMap();
    if(resources != null && resources.length != 0) {
      for (ResourceInformation resInfo : resources) {
        ResourceInformationProto.Builder e = ResourceInformationProto
            .newBuilder();
        e.setKey(resInfo.getName());
        e.setUnits(resInfo.getUnits());
        e.setType(ProtoUtils.converToProtoFormat(resInfo.getResourceType()));
        e.setValue(resInfo.getValue());
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
