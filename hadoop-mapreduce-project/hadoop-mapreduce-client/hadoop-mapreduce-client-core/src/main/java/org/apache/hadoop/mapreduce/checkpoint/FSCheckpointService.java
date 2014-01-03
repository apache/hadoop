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
package org.apache.hadoop.mapreduce.checkpoint;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A FileSystem based CheckpointService.
 */
public class FSCheckpointService implements CheckpointService {

  private final Path base;
  private final FileSystem fs;
  private final CheckpointNamingService namingPolicy;
  private final short replication;

  public FSCheckpointService(FileSystem fs, Path base,
      CheckpointNamingService namingPolicy, short replication) {
    this.fs = fs;
    this.base = base;
    this.namingPolicy = namingPolicy;
    this.replication = replication;
  }

  public CheckpointWriteChannel create()
      throws IOException {

    String name = namingPolicy.getNewName();

    Path p = new Path(name);
    if (p.isUriPathAbsolute()) {
      throw new IOException("Checkpoint cannot be an absolute path");
    }
    return createInternal(new Path(base, p));
  }

  CheckpointWriteChannel createInternal(Path name) throws IOException {

    //create a temp file, fail if file exists
    return new FSCheckpointWriteChannel(name, fs.create(tmpfile(name),
          replication));
  }

  private static class FSCheckpointWriteChannel
      implements CheckpointWriteChannel {
    private boolean isOpen = true;
    private final Path finalDst;
    private final WritableByteChannel out;

    FSCheckpointWriteChannel(Path finalDst, FSDataOutputStream out) {
      this.finalDst = finalDst;
      this.out = Channels.newChannel(out);
    }

    public int write(ByteBuffer b) throws IOException {
      return out.write(b);
    }

    public Path getDestination() {
      return finalDst;
    }

    @Override
    public void close() throws IOException {
      isOpen=false;
      out.close();
    }

    @Override
    public boolean isOpen() {
      return isOpen;
    }

  }

  @Override
  public CheckpointReadChannel open(CheckpointID id)
      throws IOException, InterruptedException {
      if (!(id instanceof FSCheckpointID)) {
        throw new IllegalArgumentException(
            "Mismatched checkpoint type: " + id.getClass());
      }
      return new FSCheckpointReadChannel(
          fs.open(((FSCheckpointID) id).getPath()));
  }

  private static class FSCheckpointReadChannel
      implements CheckpointReadChannel {

    private boolean isOpen = true;
    private final ReadableByteChannel in;

    FSCheckpointReadChannel(FSDataInputStream in){
      this.in = Channels.newChannel(in);
    }

    @Override
    public int read(ByteBuffer bb) throws IOException {
      return in.read(bb);
    }

    @Override
    public void close() throws IOException {
      isOpen = false;
      in.close();
    }

    @Override
    public boolean isOpen() {
      return isOpen;
    }

  }

  @Override
  public CheckpointID commit(CheckpointWriteChannel ch)
      throws IOException, InterruptedException {
    if (ch.isOpen()) {
      ch.close();
    }
    FSCheckpointWriteChannel hch = (FSCheckpointWriteChannel)ch;
    Path dst = hch.getDestination();
    if (!fs.rename(tmpfile(dst), dst)) {
      // attempt to clean up
      abort(ch);
      throw new IOException("Failed to promote checkpoint" +
      		 tmpfile(dst) + " -> " + dst);
    }
    return new FSCheckpointID(hch.getDestination());
  }

  @Override
  public void abort(CheckpointWriteChannel ch) throws IOException {
    if (ch.isOpen()) {
      ch.close();
    }
    FSCheckpointWriteChannel hch = (FSCheckpointWriteChannel)ch;
    Path tmp = tmpfile(hch.getDestination());
    try {
      if (!fs.delete(tmp, false)) {
        throw new IOException("Failed to delete checkpoint during abort");
      }
    } catch (FileNotFoundException e) {
      // IGNORE
    }
  }

  @Override
  public boolean delete(CheckpointID id) throws IOException,
      InterruptedException {
    if (!(id instanceof FSCheckpointID)) {
      throw new IllegalArgumentException(
          "Mismatched checkpoint type: " + id.getClass());
    }
    Path tmp = ((FSCheckpointID)id).getPath();
    try {
      return fs.delete(tmp, false);
    } catch (FileNotFoundException e) {
      // IGNORE
    }
    return true;
  }

  static final Path tmpfile(Path p) {
    return new Path(p.getParent(), p.getName() + ".tmp");
  }

}
