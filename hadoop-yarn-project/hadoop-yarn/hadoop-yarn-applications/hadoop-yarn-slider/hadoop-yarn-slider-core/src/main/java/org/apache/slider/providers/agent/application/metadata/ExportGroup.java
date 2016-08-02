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

import org.apache.slider.core.exceptions.SliderException;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ExportGroup implements Validate {
  String name;
  List<Export> exports;

  public ExportGroup() {
    exports = new ArrayList<Export>();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void addExport(Export export) {
    exports.add(export);
  }

  public List<Export> getExports() {
    return exports;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("{");
    sb.append(",\n\"name\": ").append(name);
    sb.append(",\n\"exports\" : {");
    for (Export export : exports) {
      sb.append("\n").append(export);
    }
    sb.append("\n},");
    sb.append('}');
    return sb.toString();
  }

  public void validate(String version) throws SliderException {
    Metainfo.checkNonNull(getName(), "name", "exportGroup");
    for(Export exp : getExports()) {
      exp.validate(version);
    }
  }
}
