/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.EtagSource;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.util.Preconditions;

/**
 * {@link LocatedFileStatus} extended to also carry an ETag.
 */
public class AbfsLocatedFileStatus extends LocatedFileStatus implements EtagSource {

  private static final long serialVersionUID = -8185960773314341594L;

  /**
   * etag; may be null.
   */
  private final String etag;

  public AbfsLocatedFileStatus(FileStatus status, BlockLocation[] locations) {
    super(Preconditions.checkNotNull(status), locations);
    if (status instanceof EtagSource) {
      this.etag = ((EtagSource) status).getEtag();
    } else {
      this.etag = null;
    }
  }

  @Override
  public String getEtag() {
    return etag;
  }

  @Override
  public String toString() {
    return "AbfsLocatedFileStatus{"
        + "etag='" + etag + '\'' + "} "
        + super.toString();
  }
  // equals() and hashCode() overridden to avoid FindBugs warning.
  // Base implementation is equality on Path only, which is still appropriate.

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

}
