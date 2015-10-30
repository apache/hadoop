/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.executor;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Encapsulates information required for deletions as a given user.
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class DeletionAsUserContext {
  private final String user;
  private final Path subDir;
  private final List<Path> basedirs;

  public static final class Builder {
    private String user;
    private Path subDir;
    private List<Path> basedirs;

    public Builder() {
    }

    public Builder setUser(String user) {
      this.user = user;
      return this;
    }

    public Builder setSubDir(Path subDir) {
      this.subDir = subDir;
      return this;
    }

    public Builder setBasedirs(Path... basedirs) {
      this.basedirs = Arrays.asList(basedirs);
      return this;
    }

    public DeletionAsUserContext build() {
      return new DeletionAsUserContext(this);
    }
  }

  private DeletionAsUserContext(Builder builder) {
    this.user = builder.user;
    this.subDir = builder.subDir;
    this.basedirs = builder.basedirs;
  }

  public String getUser() {
    return this.user;
  }

  public Path getSubDir() {
    return this.subDir;
  }

  public List<Path> getBasedirs() {
    if (this.basedirs != null) {
      return Collections.unmodifiableList(this.basedirs);
    } else {
      return null;
    }
  }
}