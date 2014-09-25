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

package org.apache.hadoop.conf;

import java.util.Map;
import java.util.Collection;
import java.util.HashMap;

public class ReconfigurationUtil {

  public static class PropertyChange {
    public String prop;
    public String oldVal;
    public String newVal;

    public PropertyChange(String prop, String newVal, String oldVal) {
      this.prop = prop;
      this.newVal = newVal;
      this.oldVal = oldVal;
    }
  }

  public static Collection<PropertyChange> 
    getChangedProperties(Configuration newConf, Configuration oldConf) {
    Map<String, PropertyChange> changes = new HashMap<String, PropertyChange>();

    // iterate over old configuration
    for (Map.Entry<String, String> oldEntry: oldConf) {
      String prop = oldEntry.getKey();
      String oldVal = oldEntry.getValue();
      String newVal = newConf.getRaw(prop);
      
      if (newVal == null || !newVal.equals(oldVal)) {
        changes.put(prop, new PropertyChange(prop, newVal, oldVal));
      }
    }
    
    // now iterate over new configuration
    // (to look for properties not present in old conf)
    for (Map.Entry<String, String> newEntry: newConf) {
      String prop = newEntry.getKey();
      String newVal = newEntry.getValue();
      if (oldConf.get(prop) == null) {
        changes.put(prop, new PropertyChange(prop, newVal, null));
      }
    } 

    return changes.values();
  }

  public Collection<PropertyChange> parseChangedProperties(
      Configuration newConf, Configuration oldConf) {
    return getChangedProperties(newConf, oldConf);
  }
}