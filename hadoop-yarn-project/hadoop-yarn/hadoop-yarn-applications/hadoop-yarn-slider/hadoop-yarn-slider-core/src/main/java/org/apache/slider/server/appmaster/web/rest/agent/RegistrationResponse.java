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
package org.apache.slider.server.appmaster.web.rest.agent;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class RegistrationResponse {

  @JsonProperty("response")
  private RegistrationStatus response;

  /**
   * exitstatus is a code of error which was rised on server side. exitstatus
   * = 0 (OK - Default) exitstatus = 1 (Registration failed because different
   * version of agent and server)
   */
  @JsonProperty("exitstatus")
  private int exitstatus;

  /** log - message, which will be printed to agents log */
  @JsonProperty("log")
  private String log;

  /** tags - tags associated with the container */
  @JsonProperty("tags")
  private String tags;
  
  @JsonProperty("package")
  private String pkg;

  //Response id to start with, usually zero.
  @JsonProperty("responseId")
  private long responseId;

  @JsonProperty("statusCommands")
  private List<StatusCommand> statusCommands = null;

  public RegistrationResponse() {
  }

  public RegistrationStatus getResponse() {
    return response;
  }

  public void setResponse(RegistrationStatus response) {
    this.response = response;
  }

  public int getExitstatus() {
    return exitstatus;
  }

  public void setExitstatus(int exitstatus) {
    this.exitstatus = exitstatus;
  }

  public RegistrationStatus getResponseStatus() {
    return response;
  }

  public void setResponseStatus(RegistrationStatus response) {
    this.response = response;
  }

  public List<StatusCommand> getStatusCommands() {
    return statusCommands;
  }

  public void setStatusCommands(List<StatusCommand> statusCommands) {
    this.statusCommands = statusCommands;
  }

  public long getResponseId() {
    return responseId;
  }

  public void setResponseId(long responseId) {
    this.responseId = responseId;
  }

  public String getTags() {
    return tags;
  }

  public void setTags(String tags) {
    this.tags = tags;
  }

  public String getLog() {
    return log;
  }

  public void setLog(String log) {
    this.log = log;
  }

  public String getPkg() {
    return pkg;
  }

  public void setPkg(String pkg) {
    this.pkg = pkg;
  }

  @Override
  public String toString() {
    return "RegistrationResponse{" +
           "response=" + response +
           ", responseId=" + responseId +
           ", statusCommands=" + statusCommands +
           '}';
  }
}
