/*
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

package org.apache.slider.api.types;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

/**
 * Description of a slider instance
 */
public class SliderInstanceDescription {

  public final String name;
  public final Path path;
  public final ApplicationReport applicationReport;

  public SliderInstanceDescription(String name,
      Path path,
      ApplicationReport applicationReport) {
    this.name = name;
    this.path = path;
    this.applicationReport = applicationReport;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("SliderInstanceDescription{");
    sb.append("name='").append(name).append('\'');
    sb.append(", path=").append(path);
    sb.append(", applicationReport: ")
      .append(applicationReport == null
              ? "null"
              : (" id " + applicationReport.getApplicationId()));
    sb.append('}');
    return sb.toString();
  }
}
