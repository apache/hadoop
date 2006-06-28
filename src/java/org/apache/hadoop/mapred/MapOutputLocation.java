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
import java.net.URL;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.Progressable;

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
  private int mapId;
  private String host;
  private int port;

  /** RPC constructor **/
  public MapOutputLocation() {
  }

  /** Construct a location. */
  public MapOutputLocation(String mapTaskId, int mapId, 
                           String host, int port) {
    this.mapTaskId = mapTaskId;
    this.mapId = mapId;
    this.host = host;
    this.port = port;
  }

  /** The map task id. */
  public String getMapTaskId() { return mapTaskId; }
  
  /**
   * Get the map's id number.
   * @return The numeric id for this map
   */
  public int getMapId() {
    return mapId;
  }

  /** The host the task completed on. */
  public String getHost() { return host; }

  /** The port listening for {@link MapOutputProtocol} connections. */
  public int getPort() { return port; }

  public void write(DataOutput out) throws IOException {
    UTF8.writeString(out, mapTaskId);
    out.writeInt(mapId);
    UTF8.writeString(out, host);
    out.writeInt(port);
  }

  public void readFields(DataInput in) throws IOException {
    this.mapTaskId = UTF8.readString(in);
    this.mapId = in.readInt();
    this.host = UTF8.readString(in);
    this.port = in.readInt();
  }

  public String toString() {
    return "http://" + host + ":" + port + "/getMapOutput.jsp?map=" + 
            mapTaskId;
  }
  
  /**
   * Get the map output into a local file from the remote server.
   * We use the file system so that we generate checksum files on the data.
   * @param fileSys the filesystem to write the file to
   * @param localFilename the filename to write the data into
   * @param reduce the reduce id to get for
   * @param pingee a status object that wants to know when we make progress
   * @throws IOException when something goes wrong
   */
  public long getFile(FileSystem fileSys, 
                      Path localFilename, 
                      int reduce,
                      Progressable pingee) throws IOException {
    URL path = new URL(toString() + "&reduce=" + reduce);
    InputStream input = path.openConnection().getInputStream();
    OutputStream output = fileSys.create(localFilename);
    long totalBytes = 0;
    try {
      byte[] buffer = new byte[64 * 1024];
      int len = input.read(buffer);
      while (len > 0) {
        totalBytes += len;
        output.write(buffer, 0 ,len);
        if (pingee != null) {
          pingee.progress();
        }
        len = input.read(buffer);
      }
    } finally {
      input.close();
      output.close();
    }
    return totalBytes;
  }
}
