/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.IOException;

import java.io.*;
import org.apache.hadoop.io.*;

/** The location of a map output file, as passed to a reduce task via the
 * {@link InterTrackerProtocol}. */ 
class MapOutputLocation implements Writable {

    static {                                      // register a ctor
      WritableFactories.setFactory
        (MapOutputLocation.class,
         new WritableFactory() {
           public Writable newInstance() { return new MapOutputLocation(); }
         });
    }

  private String mapTaskId;
  private String host;
  private int port;

  /** RPC constructor **/
  public MapOutputLocation() {
  }

  /** Construct a location. */
  public MapOutputLocation(String mapTaskId, String host, int port) {
    this.mapTaskId = mapTaskId;
    this.host = host;
    this.port = port;
  }

  /** The map task id. */
  public String getMapTaskId() { return mapTaskId; }

  /** The host the task completed on. */
  public String getHost() { return host; }

  /** The port listening for {@link MapOutputProtocol} connections. */
  public int getPort() { return port; }

  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, mapTaskId);
    UTF8.writeString(out, host);
    out.writeInt(port);
  }

  public void readFields(DataInput in) throws IOException {
    this.mapTaskId = UTF8.readString(in);
    this.host = UTF8.readString(in);
    this.port = in.readInt();
  }

  public String toString() {
    return mapTaskId+"@"+host+":"+port;
  }

}
