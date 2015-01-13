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
import org.apache.hadoop.util.DataChecksum;
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
    public static ChecksumParam checksumParam(
        ChecksumOpt csumOpt) {
      return new ChecksumParam(csumOpt);
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

    /** This is not needed if ChecksumParam is specified. **/
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

    public static class ChecksumParam extends CreateOpts {
      private final ChecksumOpt checksumOpt;
      protected ChecksumParam(ChecksumOpt csumOpt) {
        checksumOpt = csumOpt;
      }
      public ChecksumOpt getValue() { return checksumOpt; }
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
     * @param clazz is the desired class of the opt
     * @param opts - not null - at least one opt must be passed
     * @return an opt from one of the opts of type theClass.
     *   returns null if there isn't any
     */
    static <T extends CreateOpts> T getOpt(Class<T> clazz, CreateOpts... opts) {
      if (opts == null) {
        throw new IllegalArgumentException("Null opt");
      }
      T result = null;
      for (int i = 0; i < opts.length; ++i) {
        if (opts[i].getClass() == clazz) {
          if (result != null) {
            throw new IllegalArgumentException("multiple opts varargs: " + clazz);
          }

          @SuppressWarnings("unchecked")
          T t = (T)opts[i];
          result = t;
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
    static <T extends CreateOpts> CreateOpts[] setOpt(final T newValue,
        final CreateOpts... opts) {
      final Class<?> clazz = newValue.getClass();
      boolean alreadyInOpts = false;
      if (opts != null) {
        for (int i = 0; i < opts.length; ++i) {
          if (opts[i].getClass() == clazz) {
            if (alreadyInOpts) {
              throw new IllegalArgumentException("multiple opts varargs: " + clazz);
            }
            alreadyInOpts = true;
            opts[i] = newValue;
          }
        }
      }
      CreateOpts[] resultOpt = opts;
      if (!alreadyInOpts) { // no newValue in opt
        final int oldLength = opts == null? 0: opts.length;
        CreateOpts[] newOpts = new CreateOpts[oldLength + 1];
        if (oldLength > 0) {
          System.arraycopy(opts, 0, newOpts, 0, oldLength);
        }
        newOpts[oldLength] = newValue;
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

  /**
   * This is used in FileSystem and FileContext to specify checksum options.
   */
  public static class ChecksumOpt {
    private final DataChecksum.Type checksumType;
    private final int bytesPerChecksum;

    /**
     * Create a uninitialized one
     */
    public ChecksumOpt() {
      this(DataChecksum.Type.DEFAULT, -1);
    }

    /**
     * Normal ctor
     * @param type checksum type
     * @param size bytes per checksum
     */
    public ChecksumOpt(DataChecksum.Type type, int size) {
      checksumType = type;
      bytesPerChecksum = size;
    }

    public int getBytesPerChecksum() {
      return bytesPerChecksum;
    }

    public DataChecksum.Type getChecksumType() {
      return checksumType;
    }
    
    @Override
    public String toString() {
      return checksumType + ":" + bytesPerChecksum;
    }

    /**
     * Create a ChecksumOpts that disables checksum
     */
    public static ChecksumOpt createDisabled() {
      return new ChecksumOpt(DataChecksum.Type.NULL, -1);
    }

    /**
     * A helper method for processing user input and default value to 
     * create a combined checksum option. This is a bit complicated because
     * bytesPerChecksum is kept for backward compatibility.
     *
     * @param defaultOpt Default checksum option
     * @param userOpt User-specified checksum option. Ignored if null.
     * @param userBytesPerChecksum User-specified bytesPerChecksum
     *                Ignored if < 0.
     */
    public static ChecksumOpt processChecksumOpt(ChecksumOpt defaultOpt, 
        ChecksumOpt userOpt, int userBytesPerChecksum) {
      final boolean useDefaultType;
      final DataChecksum.Type type;
      if (userOpt != null 
          && userOpt.getChecksumType() != DataChecksum.Type.DEFAULT) {
        useDefaultType = false;
        type = userOpt.getChecksumType();
      } else {
        useDefaultType = true;
        type = defaultOpt.getChecksumType();
      }

      //  bytesPerChecksum - order of preference
      //    user specified value in bytesPerChecksum
      //    user specified value in checksumOpt
      //    default.
      if (userBytesPerChecksum > 0) {
        return new ChecksumOpt(type, userBytesPerChecksum);
      } else if (userOpt != null && userOpt.getBytesPerChecksum() > 0) {
        return !useDefaultType? userOpt
            : new ChecksumOpt(type, userOpt.getBytesPerChecksum());
      } else {
        return useDefaultType? defaultOpt
            : new ChecksumOpt(type, defaultOpt.getBytesPerChecksum());
      }
    }

    /**
     * A helper method for processing user input and default value to 
     * create a combined checksum option. 
     *
     * @param defaultOpt Default checksum option
     * @param userOpt User-specified checksum option
     */
    public static ChecksumOpt processChecksumOpt(ChecksumOpt defaultOpt,
        ChecksumOpt userOpt) {
      return processChecksumOpt(defaultOpt, userOpt, -1);
    }
  }
}
