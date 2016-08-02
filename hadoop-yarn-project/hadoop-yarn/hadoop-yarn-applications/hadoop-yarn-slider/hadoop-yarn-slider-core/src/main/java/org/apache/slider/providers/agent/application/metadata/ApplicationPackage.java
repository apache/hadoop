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
package org.apache.slider.providers.agent.application.metadata;

import java.util.ArrayList;
import java.util.List;

import org.apache.slider.core.exceptions.SliderException;

public class ApplicationPackage extends AbstractMetainfoSchema{
  private List<ComponentsInAddonPackage> components = new ArrayList<ComponentsInAddonPackage>();

  public void addComponent(ComponentsInAddonPackage component) {
    components.add(component);
  }

  // we must override getcomponent() as well. otherwise it is pointing to the
  // overriden components of type List<Component>
  public List<ComponentsInAddonPackage> getComponents(){
    return this.components;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("{");
    sb.append("\n\"name\": ").append(name);
    sb.append(",\n\"comment\": ").append(comment);
    sb.append(",\n\"version\" :").append(version);
    sb.append(",\n\"components\" : {");
    for (ComponentsInAddonPackage component : components) {
      sb.append("\n").append(component);
    }
    sb.append("\n},");
    sb.append('}');
    return sb.toString();
  }

  @Override
  public void validate(String version) throws SliderException {
    if (name == null || name.isEmpty()) {
      throw new SliderException(
          "Missing name in metainfo.json for add on packages");
    }
    if (components.isEmpty()) {
      throw new SliderException(
          "Missing components in metainfo.json for add on packages");
    }
    for (ComponentsInAddonPackage component : components) {
      if (component.name == null || component.name.isEmpty()) {
        throw new SliderException(
            "Missing name of components in metainfo.json for add on packages");
      }
    }
  }
}
