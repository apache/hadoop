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

package org.apache.hadoop.runc.squashfs.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;

public class MappedFile {

  private final long fileSize;
  private final MappedByteBuffer[] buffers;
  private final int mapSize;
  private final int windowSize;

  MappedFile(long fileSize, MappedByteBuffer[] buffers, int mapSize,
      int windowSize) {
    this.fileSize = fileSize;
    this.buffers = buffers;
    this.mapSize = mapSize;
    this.windowSize = windowSize;
  }

  public static MappedFile mmap(FileChannel channel, int bufferSize,
      int windowSize) throws IOException {
    long size = channel.size();

    List<MappedByteBuffer> buffers = new ArrayList<>();

    long offset = 0L;
    long remain = size;

    while (offset < size) {
      long mapSize = Math.min(windowSize, remain);

      buffers.add(channel.map(MapMode.READ_ONLY, offset, mapSize));
      offset += bufferSize;
      remain -= bufferSize;
    }

    MappedByteBuffer[] bufferArray =
        buffers.toArray(new MappedByteBuffer[buffers.size()]);

    return new MappedFile(size, bufferArray, bufferSize, windowSize);
  }

  public long getFileSize() {
    return fileSize;
  }

  public int getMapSize() {
    return mapSize;
  }

  public int getWindowSize() {
    return windowSize;
  }

  MappedByteBuffer buffer(long offset) {
    long remain = offset % mapSize;
    int block = (int) ((offset - remain) / mapSize);
    return buffers[block];
  }

  int bufferOffset(long offset) {
    long remain = offset % mapSize;
    return (int) remain;
  }

  public ByteBuffer from(long offset) {
    MappedByteBuffer src = buffer(offset);
    int bufOffset = bufferOffset(offset);

    ByteBuffer copy = src.duplicate();
    copy.position(copy.position() + bufOffset);
    ByteBuffer slice = copy.slice();
    return slice;
  }

  @Override
  public String toString() {
    return String.format(
        "mapped-file: { size=%d, buffers=%d, mapSize=%d, windowSize=%d }",
        fileSize, buffers.length, mapSize, windowSize);
  }

}
