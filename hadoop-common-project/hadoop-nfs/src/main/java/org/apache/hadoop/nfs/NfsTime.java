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
package org.apache.hadoop.nfs;

import org.apache.hadoop.oncrpc.XDR;

/**
 * Class that encapsulates time.
 */
public class NfsTime {
  static final int MILLISECONDS_IN_SECOND = 1000;
  static final int NANOSECONDS_IN_MILLISECOND = 1000000;
  private final int seconds;
  private final int nseconds;

  public NfsTime(int seconds, int nseconds) {
    this.seconds = seconds;
    this.nseconds = nseconds;
  }

  public NfsTime(NfsTime other) {
    seconds = other.getNseconds();
    nseconds = other.getNseconds();
  }
  
  public NfsTime(long milliseconds) {
    seconds = (int) (milliseconds / MILLISECONDS_IN_SECOND);
    nseconds = (int) ((milliseconds - this.seconds * MILLISECONDS_IN_SECOND) * 
        NANOSECONDS_IN_MILLISECOND);
  }

  public int getSeconds() {
    return seconds;
  }
  
  public int getNseconds() {
    return nseconds;
  }

  /**
   * Get the total time in milliseconds
   * @return convert to milli seconds
   */
  public long getMilliSeconds() {
    return (long) (seconds) * 1000 + (long) (nseconds) / 1000000;
  }

  public void serialize(XDR xdr) {
    xdr.writeInt(getSeconds());
    xdr.writeInt(getNseconds());
  }

  public static NfsTime deserialize(XDR xdr) {
    return new NfsTime(xdr.readInt(), xdr.readInt());
  }

  @Override
  public int hashCode() {
    return seconds ^ nseconds;
  }
  
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof NfsTime)) {
      return false;
    }
    return ((NfsTime) o).getMilliSeconds() == this.getMilliSeconds();
  }
  
  @Override
  public String toString() {
    return "(NfsTime-" + seconds + "s, " + nseconds + "ns)";
  }
}
