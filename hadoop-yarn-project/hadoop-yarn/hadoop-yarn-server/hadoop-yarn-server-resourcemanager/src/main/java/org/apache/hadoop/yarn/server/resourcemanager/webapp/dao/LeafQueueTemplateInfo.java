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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

/**
 * This class stores the LeafQueue Template configuration.
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class LeafQueueTemplateInfo {

  private ArrayList<ConfItem> property = new ArrayList<>();

  public LeafQueueTemplateInfo() {
  } // JAXB needs this

  public LeafQueueTemplateInfo(Configuration conf, String queuePath) {
    String configPrefix = CapacitySchedulerConfiguration.
        getQueuePrefix(queuePath) + AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX
        + DOT;
    conf.forEach(entry -> {
      if (entry.getKey().startsWith(configPrefix)) {
        String name = entry.getKey();
        int start = name.lastIndexOf(AUTO_CREATED_LEAF_QUEUE_TEMPLATE_PREFIX
            + DOT);
        add(new ConfItem(name.substring(start), entry.getValue()));
      }
    });
  }

  public void add(ConfItem confItem) {
    property.add(confItem);
  }

  public ArrayList<ConfItem> getItems() {
    return property;
  }

  /**
   * This class stores the Configuration Property.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class ConfItem {

    private String name;
    private String value;

    public ConfItem() {
      // JAXB needs this
    }

    public ConfItem(String name, String value){
      this.name = name;
      this.value = value;
    }

    public String getKey() {
      return name;
    }

    public String getValue() {
      return value;
    }
  }
}
