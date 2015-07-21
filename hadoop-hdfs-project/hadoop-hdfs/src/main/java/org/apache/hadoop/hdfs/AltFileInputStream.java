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

package org.apache.hadoop.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import sun.nio.ch.FileChannelImpl;

import java.io.InputStream;
import java.io.FileDescriptor;
import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

/**
 * This class is substitute for FileInputStream.Cause FileInputStream cause GC
 * pause for a long time.
 */
public class AltFileInputStream extends InputStream {

  public static final Log LOG = LogFactory.getLog(AltFileInputStream.class);
  private final FileDescriptor fd;

  private final String path;

  private InputStream inputStream;
  private FileChannel channel = null;

  /**
   * Constructs a inputstream with a channel.
   * @param file
   * @throws IOException
   */
  public AltFileInputStream(File file) throws IOException{
    if (null == file)
      throw new IllegalArgumentException();
    String name = file.getPath();
    fd = new FileDescriptor();
    path = name;
    channel = FileChannelImpl.open(fd,path,true,false,this);
    inputStream = Channels.newInputStream(channel);
  }

  public FileChannel getChannel() {
    return channel;
  }

  public final FileDescriptor getFD() throws IOException {
    if (fd != null) {
      return fd;
    }
    throw new IOException();
}

  /**
   * AltFileInputStream can convert to InputStream safely.
   * @return
   */
  public static boolean toInputStream(){
    return true;
  }

  /**
   * // have buffer ? Test between AltFileInputStream and FileInputStream,if the performance is too slowly than
   * FileInputStream,in this case,test buffer.
   * @return
   * @throws IOException
   */
  @Override
  public int read() throws IOException{
    return inputStream.read();
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }
}