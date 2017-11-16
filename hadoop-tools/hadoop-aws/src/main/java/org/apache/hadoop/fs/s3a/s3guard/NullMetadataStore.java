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

package org.apache.hadoop.fs.s3a.s3guard;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A no-op implementation of MetadataStore.  Clients that use this
 * implementation should behave the same as they would without any
 * MetadataStore.
 */
public class NullMetadataStore implements MetadataStore {

  @Override
  public void initialize(FileSystem fs) throws IOException {
  }

  @Override
  public void initialize(Configuration conf) throws IOException {
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void delete(Path path) throws IOException {
  }

  @Override
  public void forgetMetadata(Path path) throws IOException {
  }

  @Override
  public void deleteSubtree(Path path) throws IOException {
  }

  @Override
  public PathMetadata get(Path path) throws IOException {
    return null;
  }

  @Override
  public PathMetadata get(Path path, boolean wantEmptyDirectoryFlag)
      throws IOException {
    return null;
  }

  @Override
  public DirListingMetadata listChildren(Path path) throws IOException {
    return null;
  }

  @Override
  public void move(Collection<Path> pathsToDelete,
      Collection<PathMetadata> pathsToCreate) throws IOException {
  }

  @Override
  public void put(PathMetadata meta) throws IOException {
  }

  @Override
  public void put(Collection<PathMetadata> meta) throws IOException {
  }

  @Override
  public void put(DirListingMetadata meta) throws IOException {
  }

  @Override
  public void destroy() throws IOException {
  }

  @Override
  public void prune(long modTime) {
  }

  @Override
  public String toString() {
    return "NullMetadataStore";
  }

  @Override
  public Map<String, String> getDiagnostics() throws IOException {
    Map<String, String> map = new HashMap<>();
    map.put("name", "Null Metadata Store");
    map.put("description", "This is not a real metadata store");
    return map;
  }

  @Override
  public void updateParameters(Map<String, String> parameters)
      throws IOException {
  }
}
