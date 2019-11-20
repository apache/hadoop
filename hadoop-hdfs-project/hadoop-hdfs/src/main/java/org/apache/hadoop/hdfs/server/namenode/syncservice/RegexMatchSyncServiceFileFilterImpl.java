/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode.syncservice;

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class RegexMatchSyncServiceFileFilterImpl implements SyncServiceFileFilter {

  private List<Pattern> excludedPaths;

  public RegexMatchSyncServiceFileFilterImpl(List<String> excludedPaths) {
    this.excludedPaths = excludedPaths.stream()
        .map(p -> Pattern.compile(p))
        .collect(Collectors.toList());
  }

  @Override
  public boolean isExcluded(File file) {
    String[] paths = file.toString().split(Path.SEPARATOR);
    return Arrays.stream(paths)
        .anyMatch(path ->
            excludedPaths.stream()
                .anyMatch(p -> {
                  Matcher m = p.matcher(path);
                  return m.matches();
                }));
  }
}
