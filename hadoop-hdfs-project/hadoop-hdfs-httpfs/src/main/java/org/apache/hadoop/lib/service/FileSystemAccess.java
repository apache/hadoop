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

package org.apache.hadoop.lib.service;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

@InterfaceAudience.Private
public interface FileSystemAccess {

  public interface FileSystemExecutor<T> {

    public T execute(FileSystem fs) throws IOException;
  }

  public <T> T execute(String user, Configuration conf, FileSystemExecutor<T> executor) throws
    FileSystemAccessException;

  public FileSystem createFileSystem(String user, Configuration conf) throws IOException, FileSystemAccessException;

  public void releaseFileSystem(FileSystem fs) throws IOException;

  public Configuration getFileSystemConfiguration();

}
