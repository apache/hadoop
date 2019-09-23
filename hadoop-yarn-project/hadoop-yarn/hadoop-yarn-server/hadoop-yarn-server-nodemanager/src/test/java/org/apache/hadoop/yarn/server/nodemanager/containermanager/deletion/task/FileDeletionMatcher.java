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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.mockito.ArgumentMatcher;

import java.util.List;

/**
 * ArgumentMatcher to check the arguments of the {@link FileDeletionTask}.
 */
public class FileDeletionMatcher implements ArgumentMatcher<FileDeletionTask> {

  private final DeletionService delService;
  private final String user;
  private final Path subDirIncludes;
  private final List<Path> baseDirIncludes;

  public FileDeletionMatcher(DeletionService delService, String user,
      Path subDirIncludes, List<Path> baseDirIncludes) {
    this.delService = delService;
    this.user = user;
    this.subDirIncludes = subDirIncludes;
    this.baseDirIncludes = baseDirIncludes;
  }

  @Override
  public boolean matches(FileDeletionTask fd) {
    if (fd.getUser() == null && user != null) {
      return false;
    } else if (fd.getUser() != null && user == null) {
      return false;
    } else if (fd.getUser() != null && user != null) {
      return fd.getUser().equals(user);
    }
    if (!comparePaths(fd.getSubDir(), subDirIncludes.getName())) {
      return false;
    }
    if (baseDirIncludes == null && fd.getBaseDirs() != null) {
      return false;
    } else if (baseDirIncludes != null && fd.getBaseDirs() == null) {
      return false;
    } else if (baseDirIncludes != null && fd.getBaseDirs() != null) {
      if (baseDirIncludes.size() != fd.getBaseDirs().size()) {
        return false;
      }
      for (int i = 0; i < baseDirIncludes.size(); i++) {
        if (!comparePaths(fd.getBaseDirs().get(i),
            baseDirIncludes.get(i).getName())) {
          return false;
        }
      }
    }
    return true;
  }

  public boolean comparePaths(Path p1, String p2) {
    if (p1 == null && p2 != null) {
      return false;
    } else if (p1 != null && p2 == null) {
      return false;
    } else if (p1 != null && p2 != null) {
      return p1.toUri().getPath().contains(p2.toString());
    }
    return true;
  }
}
