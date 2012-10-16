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

package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

/**
 * NotInMountpointException extends the UnsupportedOperationException.
 * Exception class used in cases where the given path is not mounted 
 * through viewfs.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
@SuppressWarnings("serial")
public class NotInMountpointException extends UnsupportedOperationException {
    final String msg;

    public NotInMountpointException(Path path, String operation) {
      msg = operation + " on path `" + path + "' is not within a mount point";
    }

    public NotInMountpointException(String operation) {
      msg = operation + " on empty path is invalid";
    }

    @Override
    public String getMessage() {
      return msg;
    }
}
