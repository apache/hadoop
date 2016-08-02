/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.providers.agent;

import org.apache.slider.common.tools.SliderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/** A simple tag provider that attempts to associate tags from 1-N to all container of a component */
public class ComponentTagProvider {
  private static final Logger log = LoggerFactory.getLogger(ComponentTagProvider.class);
  private static String FREE = "free";
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, String>> allTags;

  public ComponentTagProvider() {
    allTags = new ConcurrentHashMap<String, ConcurrentHashMap<String, String>>();
  }

  /**
   * Record an assigned tag to a container
   *
   * @param component
   * @param containerId
   * @param tag
   */
  public void recordAssignedTag(String component, String containerId, String tag) {
    if (SliderUtils.isSet(component) && SliderUtils.isSet(containerId)) {
      Integer key = null;
      try {
        key = Integer.valueOf(tag);
      } catch (NumberFormatException nfe) {
        //ignore
      }
      if (key != null && key > 0) {
        ConcurrentHashMap<String, String> compTags = getComponentSpecificTags(component);
        synchronized (compTags) {
          for (int index = 1; index <= key.intValue(); index++) {
            String tempKey = new Integer(index).toString();
            if (!compTags.containsKey(tempKey)) {
              compTags.put(tempKey, FREE);
            }
          }
          compTags.put(key.toString(), containerId);
        }
      }
    }
  }

  /**
   * Get a tag for container
   *
   * @param component
   * @param containerId
   *
   * @return
   */
  public String getTag(String component, String containerId) {
    if (SliderUtils.isSet(component) && SliderUtils.isSet(containerId)) {
      ConcurrentHashMap<String, String> compTags = getComponentSpecificTags(component);
      synchronized (compTags) {
        for (String key : compTags.keySet()) {
          if (compTags.get(key).equals(containerId)) {
            return key;
          }
        }
        for (String key : compTags.keySet()) {
          if (compTags.get(key).equals(FREE)) {
            compTags.put(key, containerId);
            return key;
          }
        }
        String newKey = new Integer(compTags.size() + 1).toString();
        compTags.put(newKey, containerId);
        return newKey;
      }
    }
    return "";
  }

  /**
   * Release a tag associated with a container
   *
   * @param component
   * @param containerId
   */
  public void releaseTag(String component, String containerId) {
    if (SliderUtils.isSet(component) && SliderUtils.isSet(containerId)) {
      ConcurrentHashMap<String, String> compTags = allTags.get(component);
      if (compTags != null) {
        synchronized (compTags) {
          for (String key : compTags.keySet()) {
            if (compTags.get(key).equals(containerId)) {
              compTags.put(key, FREE);
            }
          }
        }
      }
    }
  }

  private ConcurrentHashMap<String, String> getComponentSpecificTags(String component) {
    if (!allTags.containsKey(component)) {
      synchronized (allTags) {
        if (!allTags.containsKey(component)) {
          allTags.put(component, new ConcurrentHashMap<String, String>());
        }
      }
    }
    return allTags.get(component);
  }
}
