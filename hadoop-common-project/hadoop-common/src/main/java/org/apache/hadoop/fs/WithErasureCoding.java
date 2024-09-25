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

package org.apache.hadoop.fs;

import java.io.IOException;

/**
 * Filesystems that support EC can implement this interface.
 */
public interface WithErasureCoding {

  /**
   * Get the EC Policy name of the given file's fileStatus.
   * If the file is not erasure coded, this shall return null.
   * Callers will make sure to check if fileStatus isInstance of
   * an FS that implements this interface.
   * If the call fails due to some error, this shall return null.
   * @param fileStatus object of the file whose ecPolicy needs to be obtained.
   * @return the ec Policy name
   */
  String getErasureCodingPolicyName(FileStatus fileStatus);

  /**
   * Set the given ecPolicy on the path.
   * The path and ecPolicyName should be valid (not null/empty, the
   * implementing FS shall support the supplied ecPolicy).
   * implementations can throw IOException if these conditions are not met.
   * @param path on which the EC policy needs to be set.
   * @param ecPolicyName the EC policy.
   * @throws IOException if there is an error during the set op.
   */
  void setErasureCodingPolicy(Path path, String ecPolicyName) throws
      IOException;
}
