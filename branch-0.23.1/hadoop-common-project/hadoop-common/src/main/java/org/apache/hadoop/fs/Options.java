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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * This class contains options related to file system operations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
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
    public static BufferSize bufferSize(int bs) { 
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
    
    public static class BlockSize extends CreateOpts {
      private final long blockSize;
      protected BlockSize(long bs) {
        if (bs <= 0) {
          throw new IllegalArgumentException(
                        "Block size must be greater than 0");
        }
        blockSize = bs; 
      }
      public long getValue() { return blockSize; }
    }
    
    public static class ReplicationFactor extends CreateOpts {
      private final short replication;
      protected ReplicationFactor(short rf) { 
        if (rf <= 0) {
          throw new IllegalArgumentException(
                      "Replication must be greater than 0");
        }
        replication = rf;
      }
      public short getValue() { return replication; }
    }
    
    public static class BufferSize extends CreateOpts {
      private final int bufferSize;
      protected BufferSize(int bs) {
        if (bs <= 0) {
          throw new IllegalArgumentException(
                        "Buffer size must be greater than 0");
        }
        bufferSize = bs; 
      }
      public int getValue() { return bufferSize; }
    }
    
    public static class BytesPerChecksum extends CreateOpts {
      private final int bytesPerChecksum;
      protected BytesPerChecksum(short bpc) { 
        if (bpc <= 0) {
          throw new IllegalArgumentException(
                        "Bytes per checksum must be greater than 0");
        }
        bytesPerChecksum = bpc; 
      }
      public int getValue() { return bytesPerChecksum; }
    }
    
    public static class Perms extends CreateOpts {
      private final FsPermission permissions;
      protected Perms(FsPermission perm) { 
        if(perm == null) {
          throw new IllegalArgumentException("Permissions must not be null");
        }
        permissions = perm; 
      }
      public FsPermission getValue() { return permissions; }
    }
    
    public static class Progress extends CreateOpts {
      private final Progressable progress;
      protected Progress(Progressable prog) { 
        if(prog == null) {
          throw new IllegalArgumentException("Progress must not be null");
        }
        progress = prog;
      }
      public Progressable getValue() { return progress; }
    }
    
    public static class CreateParent extends CreateOpts {
      private final boolean createParent;
      protected CreateParent(boolean createPar) {
        createParent = createPar;}
      public boolean getValue() { return createParent; }
    }

    
    /**
     * Get an option of desired type
     * @param theClass is the desired class of the opt
     * @param opts - not null - at least one opt must be passed
     * @return an opt from one of the opts of type theClass.
     *   returns null if there isn't any
     */
    protected static CreateOpts getOpt(Class<? extends CreateOpts> theClass,  CreateOpts ...opts) {
      if (opts == null) {
        throw new IllegalArgumentException("Null opt");
      }
      CreateOpts result = null;
      for (int i = 0; i < opts.length; ++i) {
        if (opts[i].getClass() == theClass) {
          if (result != null) 
            throw new IllegalArgumentException("multiple blocksize varargs");
          result = opts[i];
        }
      }
      return result;
    }
    /**
     * set an option
     * @param newValue  the option to be set
     * @param opts  - the option is set into this array of opts
     * @return updated CreateOpts[] == opts + newValue
     */
    protected static <T extends CreateOpts> CreateOpts[] setOpt(T newValue,
        CreateOpts ...opts) {
      boolean alreadyInOpts = false;
      if (opts != null) {
        for (int i = 0; i < opts.length; ++i) {
          if (opts[i].getClass() == newValue.getClass()) {
            if (alreadyInOpts) 
              throw new IllegalArgumentException("multiple opts varargs");
            alreadyInOpts = true;
            opts[i] = newValue;
          }
        }
      }
      CreateOpts[] resultOpt = opts;
      if (!alreadyInOpts) { // no newValue in opt
        CreateOpts[] newOpts = new CreateOpts[opts.length + 1];
        System.arraycopy(opts, 0, newOpts, 0, opts.length);
        newOpts[opts.length] = newValue;
        resultOpt = newOpts;
      }
      return resultOpt;
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
