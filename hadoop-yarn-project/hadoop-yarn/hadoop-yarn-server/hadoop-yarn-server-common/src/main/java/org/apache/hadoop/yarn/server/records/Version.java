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

package org.apache.hadoop.yarn.server.records;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * The version information for state get stored in YARN components,
 * i.e. RMState, NMState, etc., which include: majorVersion and 
 * minorVersion.
 * The major version update means incompatible changes happen while
 * minor version update indicates compatible changes.
 */
@LimitedPrivate({"YARN", "MapReduce"})
@Unstable
public abstract class Version {

  public static Version newInstance(int majorVersion, int minorVersion) {
    Version version = Records.newRecord(Version.class);
    version.setMajorVersion(majorVersion);
    version.setMinorVersion(minorVersion);
    return version;
  }

  public abstract int getMajorVersion();

  public abstract void setMajorVersion(int majorVersion);

  public abstract int getMinorVersion();

  public abstract void setMinorVersion(int minorVersion);

  public String toString() {
    return getMajorVersion() + "." + getMinorVersion();
  }

  public boolean isCompatibleTo(Version version) {
    return getMajorVersion() == version.getMajorVersion();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + getMajorVersion();
    result = prime * result + getMinorVersion();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Version other = (Version) obj;
    if (this.getMajorVersion() == other.getMajorVersion()
        && this.getMinorVersion() == other.getMinorVersion()) {
      return true;
    } else {
      return false;
    }
  }
}
