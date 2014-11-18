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

import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class LocalResourceRequest
    extends LocalResource implements Comparable<LocalResourceRequest> {

  private final Path loc;
  private final long timestamp;
  private final LocalResourceType type;
  private final LocalResourceVisibility visibility;
  private final String pattern;

  /**
   * Wrap API resource to match against cache of localized resources.
   * @param resource Resource requested by container
   * @throws URISyntaxException If the path is malformed
   */
  public LocalResourceRequest(LocalResource resource)
      throws URISyntaxException {
    this(ConverterUtils.getPathFromYarnURL(resource.getResource()),
        resource.getTimestamp(),
        resource.getType(),
        resource.getVisibility(),
        resource.getPattern());
  }

  LocalResourceRequest(Path loc, long timestamp, LocalResourceType type,
      LocalResourceVisibility visibility, String pattern) {
    this.loc = loc;
    this.timestamp = timestamp;
    this.type = type;
    this.visibility = visibility;
    this.pattern = pattern;
  }

  @Override
  public int hashCode() {
    int hash = loc.hashCode() ^
      (int)((timestamp >>> 32) ^ timestamp) *
      type.hashCode();
    if(pattern != null) {
      hash = hash ^ pattern.hashCode();
    }
    return hash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LocalResourceRequest)) {
      return false;
    }
    final LocalResourceRequest other = (LocalResourceRequest) o;
    String pattern = getPattern();
    String otherPattern = other.getPattern();
    boolean patternEquals = (pattern == null && otherPattern == null) || 
       (pattern != null && otherPattern != null && pattern.equals(otherPattern)); 
    return getPath().equals(other.getPath()) &&
           getTimestamp() == other.getTimestamp() &&
           getType() == other.getType() &&
           patternEquals;
  }

  @Override
  public int compareTo(LocalResourceRequest other) {
    if (this == other) {
      return 0;
    }
    int ret = getPath().compareTo(other.getPath());
    if (0 == ret) {
      ret = (int)(getTimestamp() - other.getTimestamp());
      if (0 == ret) {
        ret = getType().ordinal() - other.getType().ordinal();
        if (0 == ret) {
          String pattern = getPattern();
          String otherPattern = other.getPattern();
          if (pattern == null && otherPattern == null) {
            ret = 0;
          } else if (pattern == null) {
            ret = -1;
          } else if (otherPattern == null) {
            ret = 1;
          } else {
            ret = pattern.compareTo(otherPattern);    
          }
        }
      }
    }
    return ret;
  }

  public Path getPath() {
    return loc;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public LocalResourceType getType() {
    return type;
  }

  @Override
  public URL getResource() {
    return ConverterUtils.getYarnUrlFromPath(loc);
  }

  @Override
  public long getSize() {
    return -1L;
  }

  @Override
  public LocalResourceVisibility getVisibility() {
    return visibility;
  }

  @Override
  public String getPattern() {
    return pattern;
  }
  
  @Override
  public boolean getShouldBeUploadedToSharedCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setShouldBeUploadedToSharedCache(
      boolean shouldBeUploadedToSharedCache) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setResource(URL resource) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSize(long size) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setTimestamp(long timestamp) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setType(LocalResourceType type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setVisibility(LocalResourceVisibility visibility) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void setPattern(String pattern) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ ");
    sb.append(getPath().toString()).append(", ");
    sb.append(getTimestamp()).append(", ");
    sb.append(getType()).append(", ");
    sb.append(getPattern()).append(" }");
    return sb.toString();
  }
}
