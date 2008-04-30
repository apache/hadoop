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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.InMemoryFileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.apache.hadoop.mapred.ReduceTask.ReduceCopier.ShuffleClientMetrics;
import org.apache.hadoop.util.Progressable;

/** The location of a map output file, as passed to a reduce task via the
 * {@link InterTrackerProtocol}. */ 
class MapOutputLocation implements Writable, MRConstants {

  static {                                      // register a ctor
    WritableFactories.setFactory
      (MapOutputLocation.class,
       new WritableFactory() {
         public Writable newInstance() { return new MapOutputLocation(); }
       });
  }

  private TaskAttemptID mapTaskId;
  private int mapId;
  private String host;
  private int port;
  
  // basic/unit connection timeout (in milliseconds)
  private final static int UNIT_CONNECT_TIMEOUT = 30 * 1000;
  // default read timeout (in milliseconds)
  private final static int DEFAULT_READ_TIMEOUT = 3 * 60 * 1000;

  /** RPC constructor **/
  public MapOutputLocation() {
  }

  /** Construct a location. */
  public MapOutputLocation(TaskAttemptID mapTaskId, int mapId, 
                           String host, int port) {
    this.mapTaskId = mapTaskId;
    this.mapId = mapId;
    this.host = host;
    this.port = port;
  }

  /** The map task id. */
  public TaskAttemptID getMapTaskID() { return mapTaskId; }
  
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
    mapTaskId.write(out);
    out.writeInt(mapId);
    Text.writeString(out, host);
    out.writeInt(port);
  }

  public void readFields(DataInput in) throws IOException {
    this.mapTaskId = TaskAttemptID.read(in);
    this.mapId = in.readInt();
    this.host = Text.readString(in);
    this.port = in.readInt();
  }

  @Override
  public String toString() {
    return "http://" + host + ":" + port + "/mapOutput?job=" + mapTaskId.getJobID() +
           "&map=" + mapTaskId;
  }
  
  /** 
   * The connection establishment is attempted multiple times and is given up 
   * only on the last failure. Instead of connecting with a timeout of 
   * X, we try connecting with a timeout of x < X but multiple times. 
   */
  private InputStream getInputStream(URLConnection connection, 
                                     int connectionTimeout, 
                                     int readTimeout) 
  throws IOException {
    int unit = 0;
    if (connectionTimeout < 0) {
      throw new IOException("Invalid timeout "
                            + "[timeout = " + connectionTimeout + " ms]");
    } else if (connectionTimeout > 0) {
      unit = (UNIT_CONNECT_TIMEOUT > connectionTimeout)
             ? connectionTimeout
             : UNIT_CONNECT_TIMEOUT;
    }
    // set the read timeout to the total timeout
    connection.setReadTimeout(readTimeout);
    // set the connect timeout to the unit-connect-timeout
    connection.setConnectTimeout(unit);
    while (true) {
      try {
        return connection.getInputStream();
      } catch (IOException ioe) {
        // update the total remaining connect-timeout
        connectionTimeout -= unit;

        // throw an exception if we have waited for timeout amount of time
        // note that the updated value if timeout is used here
        if (connectionTimeout == 0) {
          throw ioe;
        }

        // reset the connect timeout for the last try
        if (connectionTimeout < unit) {
          unit = connectionTimeout;
          // reset the connect time out for the final connect
          connection.setConnectTimeout(unit);
        }
      }
    }
  }

  /**
   * Get the map output into a local file (either in the inmemory fs or on the 
   * local fs) from the remote server.
   * We use the file system so that we generate checksum files on the data.
   * @param inMemFileSys the inmemory filesystem to write the file to
   * @param localFileSys the local filesystem to write the file to
   * @param shuffleMetrics the metrics context
   * @param localFilename the filename to write the data into
   * @param lDirAlloc the LocalDirAllocator object
   * @param conf the Configuration object
   * @param reduce the reduce id to get for
   * @param timeout number of milliseconds for connection timeout
   * @return the path of the file that got created
   * @throws IOException when something goes wrong
   */
  public Path getFile(InMemoryFileSystem inMemFileSys,
                      FileSystem localFileSys,
                      ShuffleClientMetrics shuffleMetrics,
                      Path localFilename, 
                      LocalDirAllocator lDirAlloc,
                      Configuration conf, int reduce,
                      int timeout, Progressable progressable) 
  throws IOException, InterruptedException {
    return getFile(inMemFileSys, localFileSys, shuffleMetrics, localFilename, 
                   lDirAlloc, conf, reduce, timeout, DEFAULT_READ_TIMEOUT, 
                   progressable);
  }

  /**
   * Get the map output into a local file (either in the inmemory fs or on the 
   * local fs) from the remote server.
   * We use the file system so that we generate checksum files on the data.
   * @param inMemFileSys the inmemory filesystem to write the file to
   * @param localFileSys the local filesystem to write the file to
   * @param shuffleMetrics the metrics context
   * @param localFilename the filename to write the data into
   * @param lDirAlloc the LocalDirAllocator object
   * @param conf the Configuration object
   * @param reduce the reduce id to get for
   * @param connectionTimeout number of milliseconds for connection timeout
   * @param readTimeout number of milliseconds for read timeout
   * @return the path of the file that got created
   * @throws IOException when something goes wrong
   */
  public Path getFile(InMemoryFileSystem inMemFileSys,
                      FileSystem localFileSys,
                      ShuffleClientMetrics shuffleMetrics,
                      Path localFilename, 
                      LocalDirAllocator lDirAlloc,
                      Configuration conf, int reduce,
                      int connectionTimeout, int readTimeout, 
                      Progressable progressable) 
  throws IOException, InterruptedException {
    boolean good = false;
    long totalBytes = 0;
    FileSystem fileSys = localFileSys;
    Thread currentThread = Thread.currentThread();
    URL path = new URL(toString() + "&reduce=" + reduce);
    try {
      URLConnection connection = path.openConnection();
      InputStream input = getInputStream(connection, connectionTimeout, 
                                         readTimeout); 
      OutputStream output = null;
      
      //We will put a file in memory if it meets certain criteria:
      //1. The size of the file should be less than 25% of the total inmem fs
      //2. There is space available in the inmem fs
      
      long length = Long.parseLong(connection.getHeaderField(MAP_OUTPUT_LENGTH));
      long inMemFSSize = inMemFileSys.getFSSize();
      long checksumLength = (int)inMemFileSys.getChecksumFileLength(
                                                  localFilename, length);
      
      boolean createInMem = false; 
      if (inMemFSSize > 0)  
        createInMem = (((float)(length + checksumLength) / inMemFSSize <= 
                        MAX_INMEM_FILESIZE_FRACTION) && 
                       inMemFileSys.reserveSpaceWithCheckSum(localFilename, length));
      if (createInMem) {
        fileSys = inMemFileSys;
      }
      else {
        //now hit the localFS to find out a suitable location for the output
        localFilename = lDirAlloc.getLocalPathForWrite(
            localFilename.toUri().getPath(), length + checksumLength, conf);
      }
      
      output = fileSys.create(localFilename);
      try {  
        try {
          byte[] buffer = new byte[64 * 1024];
          if (currentThread.isInterrupted()) {
            throw new InterruptedException();
          }
          int len = input.read(buffer);
          while (len > 0) {
            totalBytes += len;
            shuffleMetrics.inputBytes(len);
            output.write(buffer, 0 , len);
            if (currentThread.isInterrupted()) {
              throw new InterruptedException();
            }
            // indicate we're making progress
            progressable.progress();
            len = input.read(buffer);
          }
        } finally {
          output.close();
        }
      } finally {
        input.close();
      }
      good = (totalBytes == length);
      if (!good) {
        throw new IOException("Incomplete map output received for " + path +
                              " (" + totalBytes + " instead of " + length + ")"
                              );
      }
    } finally {
      if (!good) {
        try {
          fileSys.delete(localFilename, true);
          totalBytes = 0;
        } catch (Throwable th) {
          // IGNORED because we are cleaning up
        }
      }
    }
    return fileSys.makeQualified(localFilename);
  }

}
