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

package org.apache.hadoop.yarn.api.records;

import java.util.List;

public interface AMResponse {
  public boolean getReboot();
  public int getResponseId();
  
  public List<Container> getContainerList();
  public Container getContainer(int index);
  public int getContainerCount();

  public void setReboot(boolean reboot);
  public void setResponseId(int responseId);
  
  public void addAllContainers(List<Container> containers);
  public void addContainer(Container container);
  public void removeContainer(int index);
  public void clearContainers();
  
  public void setAvailableResources(Resource limit);
  public Resource getAvailableResources();
}