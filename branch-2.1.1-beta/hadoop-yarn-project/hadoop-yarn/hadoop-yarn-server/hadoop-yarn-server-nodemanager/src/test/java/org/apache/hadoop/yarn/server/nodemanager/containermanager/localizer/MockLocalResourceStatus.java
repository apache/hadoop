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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer;

import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.LocalResourceStatus;
import org.apache.hadoop.yarn.server.nodemanager.api.protocolrecords.ResourceStatusType;

public class MockLocalResourceStatus implements LocalResourceStatus {

  private LocalResource rsrc = null;
  private ResourceStatusType tag = null;
  private URL localPath = null;
  private long size = -1L;
  private SerializedException ex = null;

  MockLocalResourceStatus() { }
  MockLocalResourceStatus(LocalResource rsrc, ResourceStatusType tag,
      URL localPath, SerializedException ex) {
    this.rsrc = rsrc;
    this.tag = tag;
    this.localPath = localPath;
    this.ex = ex;
  }

  @Override
  public LocalResource getResource() { return rsrc; }
  @Override
  public ResourceStatusType getStatus() { return tag; }
  @Override
  public long getLocalSize() { return size; }
  @Override
  public URL getLocalPath() { return localPath; }
  @Override
  public SerializedException getException() { return ex; }
  @Override
  public void setResource(LocalResource rsrc) { this.rsrc = rsrc; }
  @Override
  public void setStatus(ResourceStatusType tag) { this.tag = tag; }
  @Override
  public void setLocalPath(URL localPath) { this.localPath = localPath; }
  @Override
  public void setLocalSize(long size) { this.size = size; }
  @Override
  public void setException(SerializedException ex) { this.ex = ex; }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MockLocalResourceStatus)) {
      return false;
    }
    MockLocalResourceStatus other = (MockLocalResourceStatus) o;
    return getResource().equals(other.getResource())
      && getStatus().equals(other.getStatus())
      && (null != getLocalPath()
          && getLocalPath().equals(other.getLocalPath()))
      && (null != getException()
          && getException().equals(other.getException()));
  }

  @Override
  public int hashCode() {
    return 4344;
  }
}
