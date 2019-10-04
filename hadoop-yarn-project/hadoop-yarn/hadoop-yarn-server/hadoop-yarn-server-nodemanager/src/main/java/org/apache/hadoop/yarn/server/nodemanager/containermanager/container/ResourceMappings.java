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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;

/**
 * This class is used to store assigned resource to a single container by
 * resource types.
 *
 * Assigned resource could be list of String
 *
 * For example, we can assign container to:
 * "numa": ["numa0"]
 * "gpu": ["0", "1", "2", "3"]
 * "fpga": ["1", "3"]
 *
 * This will be used for NM restart container recovery.
 */
public class ResourceMappings {

  private Map<String, AssignedResources> assignedResourcesMap = new HashMap<>();

  /**
   * Get all resource mappings.
   * @param resourceType resourceType
   * @return map of resource mapping
   */
  public List<Serializable> getAssignedResources(String resourceType) {
    AssignedResources ar = assignedResourcesMap.get(resourceType);
    if (null == ar) {
      return Collections.emptyList();
    }
    return ar.getAssignedResources();
  }

  /**
   * Adds the resources for a given resource type.
   *
   * @param resourceType Resource Type
   * @param assigned Assigned resources to add
   */
  public void addAssignedResources(String resourceType,
      AssignedResources assigned) {
    assignedResourcesMap.put(resourceType, assigned);
  }

  /**
   * Stores resources assigned to a container for a given resource type.
   */
  public static class AssignedResources implements Serializable {
    private static final long serialVersionUID = -1059491941955757926L;
    private List<Serializable> resources = Collections.emptyList();

    public List<Serializable> getAssignedResources() {
      return Collections.unmodifiableList(resources);
    }

    public void updateAssignedResources(List<Serializable> list) {
      this.resources = new ArrayList<>(list);
    }

    @SuppressWarnings("unchecked")
    public static AssignedResources fromBytes(byte[] bytes)
        throws IOException {
      ObjectInputStream ois = null;
      List<Serializable> resources;
      try {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ois = new ObjectInputStream(bis);
        resources = (List<Serializable>) ois.readObject();
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      } finally {
        IOUtils.closeQuietly(ois);
      }
      AssignedResources ar = new AssignedResources();
      ar.updateAssignedResources(resources);
      return ar;
    }

    public byte[] toBytes() throws IOException {
      ObjectOutputStream oos = null;
      byte[] bytes;
      try {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        oos = new ObjectOutputStream(bos);
        oos.writeObject(resources);
        bytes = bos.toByteArray();
      } finally {
        IOUtils.closeQuietly(oos);
      }
      return bytes;
    }
  }
}