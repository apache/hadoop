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

package org.apache.hadoop.fs.s3a.commit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;

/**
 * Path exception to use for various commit issues.
 */
public class PathCommitException extends PathIOException {
  public PathCommitException(String path, Throwable cause) {
    super(path, cause);
  }

  public PathCommitException(String path, String error) {
    super(path, error);
  }

  public PathCommitException(Path path, String error) {
    super(path != null ? path.toString() : "", error);
  }

  public PathCommitException(String path, String error, Throwable cause) {
    super(path, error, cause);
  }
}
