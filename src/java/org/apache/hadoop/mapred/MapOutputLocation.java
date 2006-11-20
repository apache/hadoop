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

package org.apache.hadoop.mapred;

import java.io.IOException;

import java.io.*;
import java.net.*;
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
    return "http://" + host + ":" + port + "/mapOutput?map=" + 
            mapTaskId;
  }
  
  /**
   * Get the map output into a local file from the remote server.
   * We use the file system so that we generate checksum files on the data.
   * @param fileSys the filesystem to write the file to
   * @param localFilename the filename to write the data into
   * @param reduce the reduce id to get for
   * @param pingee a status object that wants to know when we make progress
   * @param timeout number of ms for connection and read timeout
   * @throws IOException when something goes wrong
   */
  public long getFile(FileSystem fileSys, 
                      Path localFilename, 
                      int reduce,
                      Progressable pingee,
                      int timeout) throws IOException, InterruptedException {
    boolean good = false;
    long totalBytes = 0;
    Thread currentThread = Thread.currentThread();
    URL path = new URL(toString() + "&reduce=" + reduce);
    try {
      URLConnection connection = path.openConnection();
      if (timeout > 0) {
        connection.setConnectTimeout(timeout);
        connection.setReadTimeout(timeout);
      }
      InputStream input = connection.getInputStream();
      try {
        OutputStream output = fileSys.create(localFilename);
        try {
          byte[] buffer = new byte[64 * 1024];
          if (currentThread.isInterrupted()) {
            throw new InterruptedException();
          }
          int len = input.read(buffer);
          while (len > 0) {
            totalBytes += len;
            output.write(buffer, 0 ,len);
            if (pingee != null) {
              pingee.progress();
            }
            if (currentThread.isInterrupted()) {
              throw new InterruptedException();
            }
            len = input.read(buffer);
          }
        } finally {
          output.close();
        }
      } finally {
        input.close();
      }
      good = ((int) totalBytes) == connection.getContentLength();
      if (!good) {
        throw new IOException("Incomplete map output received for " + path +
                              " (" + totalBytes + " instead of " + 
                              connection.getContentLength() + ")");
      }
    } finally {
      if (!good) {
        try {
          fileSys.delete(localFilename);
          totalBytes = 0;
        } catch (Throwable th) {
          // IGNORED because we are cleaning up
        }
      }
    }
    return totalBytes;
  }
}
