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

package org.apache.hadoop.yarn.service.api.records;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.LocalizationState;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

/**
 * The status of localization.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
@ApiModel(description = "Localization status of a resource.")
@XmlRootElement
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LocalizationStatus implements Serializable {

  private static final long serialVersionUID = -5745287278502373531L;

  private String destFile;

  private LocalizationState state;

  private String diagnostics;

  /**
   * Destination file.
   */
  @JsonProperty("dest_file")
  public String getDestFile() {
    return destFile;
  }

  /**
   * Sets the destination file.
   *
   * @param destFile destination file
   */
  @XmlElement(name = "dest_file")
  public void setDestFile(String destFile) {
    this.destFile = destFile;
  }

  /**
   * Sets the destination file and returns the localization status.
   *
   * @param fileName destination file
   */
  public LocalizationStatus destFile(String fileName) {
    this.destFile = fileName;
    return this;
  }

  /**
   * Localization state.
   */
  @JsonProperty("state")
  public LocalizationState getState() {
    return state;
  }

  /**
   * Sets the localization state.
   *
   * @param localizationState localization state
   */
  @XmlElement(name = "state")
  public void setState(LocalizationState localizationState) {
    this.state = localizationState;
  }

  /**
   * Sets the localization state and returns the localization status.
   *
   * @param localizationState localization state
   */
  public LocalizationStatus state(LocalizationState localizationState) {
    this.state = localizationState;
    return this;
  }

  /**
   * Diagnostics.
   */
  @JsonProperty("diagnostics")
  public String getDiagnostics() {
    return diagnostics;
  }

  /**
   * Sets the diagnostics.
   *
   * @param diag diagnostics
   */
  @XmlElement(name = "diagnostics")
  public void setDiagnostics(String diag) {
    this.diagnostics = diag;
  }

  /**
   * Sets the diagnostics and returns the localization status.
   *
   * @param diag diagnostics
   */
  public LocalizationStatus diagnostics(String diag) {
    this.diagnostics = diag;
    return this;
  }
}
