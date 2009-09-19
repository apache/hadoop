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
package org.apache.hadoop.fs;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * This class contains options related to file system operations.
 */
public final class Options {
  /**
   * Class to support the varargs for create() options.
   *
   */
  public static class CreateOpts {
    private CreateOpts() { };
    public static BlockSize blockSize(long bs) { 
      return new BlockSize(bs);
    }
    public static BufferSize bufferSize(short bs) { 
      return new BufferSize(bs);
    }
    public static ReplicationFactor repFac(short rf) { 
      return new ReplicationFactor(rf);
    }
    public static BytesPerChecksum bytesPerChecksum(short crc) {
      return new BytesPerChecksum(crc);
    }
    public static Perms perms(FsPermission perm) {
      return new Perms(perm);
    }
    public static CreateParent createParent() {
      return new CreateParent(true);
    }
    public static CreateParent donotCreateParent() {
      return new CreateParent(false);
    }
    
    static class BlockSize extends CreateOpts {
      private final long blockSize;
      protected BlockSize(long bs) {
        if (bs <= 0) {
          throw new IllegalArgumentException(
                        "Block size must be greater than 0");
        }
        blockSize = bs; 
      }
      long getValue() { return blockSize; }
    }
    
    static class ReplicationFactor extends CreateOpts {
      private final short replication;
      protected ReplicationFactor(short rf) { 
        if (rf <= 0) {
          throw new IllegalArgumentException(
                      "Replication must be greater than 0");
        }
        replication = rf;
      }
      short getValue() { return replication; }
    }
    
    static class BufferSize extends CreateOpts {
      private final int bufferSize;
      protected BufferSize(short bs) {
        if (bs <= 0) {
          throw new IllegalArgumentException(
                        "Buffer size must be greater than 0");
        }
        bufferSize = bs; 
      }
      int getValue() { return bufferSize; }
    }
    
    static class BytesPerChecksum extends CreateOpts {
      private final int bytesPerChecksum;
      protected BytesPerChecksum(short bpc) { 
        if (bpc <= 0) {
          throw new IllegalArgumentException(
                        "Bytes per checksum must be greater than 0");
        }
        bytesPerChecksum = bpc; 
      }
      int getValue() { return bytesPerChecksum; }
    }
    
    static class Perms extends CreateOpts {
      private final FsPermission permissions;
      protected Perms(FsPermission perm) { 
        if(perm == null) {
          throw new IllegalArgumentException("Permissions must not be null");
        }
        permissions = perm; 
      }
      FsPermission getValue() { return permissions; }
    }
    
    static class Progress extends CreateOpts {
      private final Progressable progress;
      protected Progress(Progressable prog) { 
        if(prog == null) {
          throw new IllegalArgumentException("Progress must not be null");
        }
        progress = prog;
      }
      Progressable getValue() { return progress; }
    }
    
    static class CreateParent extends CreateOpts {
      private final Boolean createParent;
      protected CreateParent(boolean createPar) {
        createParent = createPar;}
      Boolean getValue() { return createParent; }
    }
  }

  /**
   * Enum to support the varargs for rename() options
   */
  public static enum Rename {
    NONE((byte) 0), // No options
    OVERWRITE((byte) 1); // Overwrite the rename destination

    private final byte code;
    
    private Rename(byte code) {
      this.code = code;
    }

    public static Rename valueOf(byte code) {
      return code < 0 || code >= values().length ? null : values()[code];
    }

    public byte value() {
      return code;
    }
  }
}
