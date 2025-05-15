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

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    public static Progress progress(Progressable prog) {
      return new Progress(prog);
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
  public enum Rename {
    NONE((byte) 0), // No options
    OVERWRITE((byte) 1), // Overwrite the rename destination
    TO_TRASH ((byte) 2); // Rename to trash

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
     * Create a ChecksumOpts that disables checksum.
     *
     * @return ChecksumOpt.
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
     *                Ignored if {@literal <} 0.
     * @return ChecksumOpt.
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
     *
     * @return ChecksumOpt.
     */
    public static ChecksumOpt processChecksumOpt(ChecksumOpt defaultOpt,
        ChecksumOpt userOpt) {
      return processChecksumOpt(defaultOpt, userOpt, -1);
    }
  }

  /**
   * Options for creating {@link PathHandle} references.
   */
  public static class HandleOpt {
    protected HandleOpt() {
    }

    /**
     * Utility function for mapping {@link FileSystem#getPathHandle} to a
     * fixed set of handle options.
     * @param fs Target filesystem
     * @param opt Options to bind in partially evaluated function
     * @return Function reference with options fixed
     */
    public static Function<FileStatus, PathHandle> resolve(
        FileSystem fs, HandleOpt... opt) {
      return resolve(fs::getPathHandle, opt);
    }

    /**
     * Utility function for partial evaluation of {@link FileStatus}
     * instances to a fixed set of handle options.
     * @param fsr Function reference
     * @param opt Options to associate with {@link FileStatus} instances to
     *            produce {@link PathHandle} instances.
     * @return Function reference with options fixed
     */
    public static Function<FileStatus, PathHandle> resolve(
        BiFunction<FileStatus, HandleOpt[], PathHandle> fsr,
        HandleOpt... opt) {
      return (stat) -> fsr.apply(stat, opt);
    }

    /**
     * Handle is valid iff the referent is neither moved nor changed.
     * Equivalent to changed(false), moved(false).
     * @return Options requiring that the content and location of the entity
     * be unchanged between calls.
     */
    public static HandleOpt[] exact() {
      return new HandleOpt[] {changed(false), moved(false) };
    }

    /**
     * Handle is valid iff the content of the referent is the same.
     * Equivalent to changed(false), moved(true).
     * @return Options requiring that the content of the entity is unchanged,
     * but it may be at a different location.
     */
    public static HandleOpt[] content() {
      return new HandleOpt[] {changed(false), moved(true)  };
    }

    /**
     * Handle is valid iff the referent is unmoved in the namespace.
     * Equivalent to changed(true), moved(false).
     * @return Options requiring that the referent exist in the same location,
     * but its content may have changed.
     */
    public static HandleOpt[] path() {
      return new HandleOpt[] {changed(true),  moved(false) };
    }

    /**
     * Handle is valid iff the referent exists in the namespace.
     * Equivalent to changed(true), moved(true).
     * @return Options requiring that the implementation resolve a reference
     * to this entity regardless of changes to content or location.
     */
    public static HandleOpt[] reference() {
      return new HandleOpt[] {changed(true),  moved(true)  };
    }

    /**
     * @param allow If true, resolve references to this entity even if it has
     *             been modified.
     * @return Handle option encoding parameter.
     */
    public static Data changed(boolean allow) {
      return new Data(allow);
    }

    /**
     * @param allow If true, resolve references to this entity anywhere in
     *              the namespace.
     * @return Handle option encoding parameter.
     */
    public static Location moved(boolean allow) {
      return new Location(allow);
    }

    /**
     * Utility method to extract a HandleOpt from the set provided.
     * @param c Target class
     * @param opt List of options
     * @param <T> Type constraint for exact match
     * @throws IllegalArgumentException If more than one matching type is found.
     * @return An option assignable from the specified type or null if either
     * opt is null or a suitable match is not found.
     */
    public static <T extends HandleOpt> Optional<T> getOpt(
        Class<T> c, HandleOpt... opt) {
      if (null == opt) {
        return Optional.empty();
      }
      T ret = null;
      for (HandleOpt o : opt) {
        if (c.isAssignableFrom(o.getClass())) {
          if (ret != null) {
            throw new IllegalArgumentException("Duplicate option "
                + c.getSimpleName());
          }

          @SuppressWarnings("unchecked")
          T tmp = (T) o;
          ret = tmp;
        }
      }
      return Optional.ofNullable(ret);
    }

    /**
     * Option storing standard constraints on data.
     */
    public static class Data extends HandleOpt {
      private final boolean allowChanged;
      Data(boolean allowChanged) {
        this.allowChanged = allowChanged;
      }

      /**
       * Tracks whether any changes to file content are permitted.
       * @return True if content changes are allowed, false otherwise.
       */
      public boolean allowChange() {
        return allowChanged;
      }
      @Override
      public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("data(allowChange=")
          .append(allowChanged).append(")");
        return sb.toString();
      }
    }

    /**
     * Option storing standard constraints on location.
     */
    public static class Location extends HandleOpt {
      private final boolean allowChanged;
      Location(boolean allowChanged) {
        this.allowChanged = allowChanged;
      }

      /**
       * Tracks whether any changes to file location are permitted.
       * @return True if relocation in the namespace is allowed, false
       * otherwise.
       */
      public boolean allowChange() {
        return allowChanged;
      }
      @Override
      public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("loc(allowChange=")
            .append(allowChanged).append(")");
        return sb.toString();
      }
    }

  }

  /**
   * Enum for indicating what mode to use when combining chunk and block
   * checksums to define an aggregate FileChecksum. This should be considered
   * a client-side runtime option rather than a persistent property of any
   * stored metadata, which is why this is not part of ChecksumOpt, which
   * deals with properties of files at rest.
   */
  public enum ChecksumCombineMode {
    MD5MD5CRC,  // MD5 of block checksums, which are MD5 over chunk CRCs
    COMPOSITE_CRC  // Block/chunk-independent composite CRC
  }

  /**
   * The standard {@code openFile()} options.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static final class OpenFileOptions {

    private OpenFileOptions() {
    }

    /**
     * Prefix for all standard filesystem options: {@value}.
     */
    private static final String FILESYSTEM_OPTION = "fs.option.";

    /**
     * Prefix for all openFile options: {@value}.
     */
    public static final String FS_OPTION_OPENFILE =
        FILESYSTEM_OPTION + "openfile.";

    /**
     * OpenFile option for file length: {@value}.
     */
    public static final String FS_OPTION_OPENFILE_LENGTH =
        FS_OPTION_OPENFILE + "length";

    /**
     * OpenFile option for split start: {@value}.
     */
    public static final String FS_OPTION_OPENFILE_SPLIT_START =
        FS_OPTION_OPENFILE + "split.start";

    /**
     * OpenFile option for split end: {@value}.
     */
    public static final String FS_OPTION_OPENFILE_SPLIT_END =
        FS_OPTION_OPENFILE + "split.end";

    /**
     * OpenFile option for buffer size: {@value}.
     */
    public static final String FS_OPTION_OPENFILE_BUFFER_SIZE =
        FS_OPTION_OPENFILE + "buffer.size";

    /**
     * OpenFile footer cache flag: {@value}.
     */
    public static final String FS_OPTION_OPENFILE_FOOTER_CACHE =
        FS_OPTION_OPENFILE + "footer.cache";

    /**
     * OpenFile option for read policies: {@value}.
     */
    public static final String FS_OPTION_OPENFILE_READ_POLICY =
        FS_OPTION_OPENFILE + "read.policy";

    /**
     * Set of standard options which openFile implementations
     * MUST recognize, even if they ignore the actual values.
     */
    public static final Set<String> FS_OPTION_OPENFILE_STANDARD_OPTIONS =
        Collections.unmodifiableSet(Stream.of(
                FS_OPTION_OPENFILE_BUFFER_SIZE,
                FS_OPTION_OPENFILE_FOOTER_CACHE,
                FS_OPTION_OPENFILE_READ_POLICY,
                FS_OPTION_OPENFILE_LENGTH,
                FS_OPTION_OPENFILE_SPLIT_START,
                FS_OPTION_OPENFILE_SPLIT_END)
            .collect(Collectors.toSet()));

    /**
     * Read policy for adaptive IO: {@value}.
     */
    public static final String FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE =
        "adaptive";

    /**
     * We are an avro file: {@value}.
     */
    public static final String FS_OPTION_OPENFILE_READ_POLICY_AVRO = "avro";

    /**
     * This is a columnar file format.
     * Do whatever is needed to optimize for it: {@value}.
     */
    public static final String FS_OPTION_OPENFILE_READ_POLICY_COLUMNAR =
        "columnar";

    /**
     * This is a CSV file of plain or UTF-8 text
     * to be read sequentially.
     * Do whatever is needed to optimize for it: {@value}.
     */
    public static final String FS_OPTION_OPENFILE_READ_POLICY_CSV =
        "csv";

    /**
     * Read policy {@value} -whatever the implementation does by default.
     */
    public static final String FS_OPTION_OPENFILE_READ_POLICY_DEFAULT =
        "default";

    /**
     * This is a table file for Apache HBase.
     * Do whatever is needed to optimize for it: {@value}.
     */
    public static final String FS_OPTION_OPENFILE_READ_POLICY_HBASE =
        "hbase";

    /**
     * This is a JSON file of UTF-8 text, including a
     * JSON line file where each line is a JSON entity.
     * Do whatever is needed to optimize for it: {@value}.
     */
    public static final String FS_OPTION_OPENFILE_READ_POLICY_JSON =
        "json";

    /**
     * This is an ORC file.
     * Do whatever is needed to optimize for it: {@value}.
     */
    public static final String FS_OPTION_OPENFILE_READ_POLICY_ORC =
        "orc";

    /**
     * This is a parquet file with a v1/v3 footer: {@value}.
     * Do whatever is needed to optimize for it, such as footer
     * prefetch and cache,
     */
    public static final String FS_OPTION_OPENFILE_READ_POLICY_PARQUET =
        "parquet";

    /**
     * Read policy for random IO: {@value}.
     */
    public static final String FS_OPTION_OPENFILE_READ_POLICY_RANDOM =
        "random";

    /**
     * Read policy for sequential IO: {@value}.
     */
    public static final String FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL =
        "sequential";

    /**
     * Vectored IO API to be used: {@value}.
     */
    public static final String FS_OPTION_OPENFILE_READ_POLICY_VECTOR =
        "vector";

    /**
     * Whole file to be read, end-to-end: {@value}.
     */
    public static final String FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE =
        "whole-file";

    /**
     * All the current read policies as a set.
     */
    public static final Set<String> FS_OPTION_OPENFILE_READ_POLICIES =
        Collections.unmodifiableSet(Stream.of(
                FS_OPTION_OPENFILE_READ_POLICY_ADAPTIVE,
                FS_OPTION_OPENFILE_READ_POLICY_AVRO,
                FS_OPTION_OPENFILE_READ_POLICY_COLUMNAR,
                FS_OPTION_OPENFILE_READ_POLICY_CSV,
                FS_OPTION_OPENFILE_READ_POLICY_DEFAULT,
                FS_OPTION_OPENFILE_READ_POLICY_JSON,
                FS_OPTION_OPENFILE_READ_POLICY_ORC,
                FS_OPTION_OPENFILE_READ_POLICY_PARQUET,
                FS_OPTION_OPENFILE_READ_POLICY_RANDOM,
                FS_OPTION_OPENFILE_READ_POLICY_SEQUENTIAL,
                FS_OPTION_OPENFILE_READ_POLICY_VECTOR,
                FS_OPTION_OPENFILE_READ_POLICY_WHOLE_FILE)
            .collect(Collectors.toSet()));

    /**
     * EC policy to be set on the file that needs to be created : {@value}.
     */
    public static final String FS_OPTION_OPENFILE_EC_POLICY =
        FS_OPTION_OPENFILE + "ec.policy";
  }

  /**
   * The standard {@code createFile()} options.
   * <p>
   * If an option is not supported during file creation and it is considered
   * part of a commit protocol, then, when supplied in a must() option,
   * it MUST be rejected.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public interface CreateFileOptionKeys {

    /**
     * {@code createFile()} option to write a file in the close() operation iff
     * there is nothing at the destination.
     * this is the equivalent of {@code create(path, overwrite=true)}
     * <i>except that the existence check is postponed to the end of the write</i>.
     * <p>
     * Value {@value}.
     * </p>
     * <p>
     * This can be set in the builder.
     * </p>
     * <ol>
     *     <li>It is for object stores stores which only upload/manifest files
     *         at the end of the stream write.</li>
     *     <li>Streams which support it SHALL not manifest any object to
     *         the destination path until close()</li>
     *     <li>It MUST be declared as a stream capability in streams for which
     *         this overwrite is enabled.</li>
     *     <li>It MUST be exported as a path capability for all stores where
     *         the feature is available <i>and</i> enabled</li>
     *     <li>If passed to a filesystem as a {@code must()} parameter where
     *         the option value is {@code true}, and it is supported/enabled,
     *         the FS SHALL omit all overwrite checks in {@code create},
     *         including for the existence of an object or a directory underneath.
     *         Instead, during {@code close()} the object will only be manifest
     *         at the target path if there is no object at the destination.
     *     </li>
     *     <li>The existence check and object creation SHALL be atomic.</li>
     *     <li>If passed to a filesystem as a {@code must()} parameter where
     *         the option value is {@code true}, and the FS does not recognise
     *         the feature, or it is recognized but disabled on this FS instance,
     *         the filesystem SHALL reject the request.
     *     </li>
     *     <li>If passed to a filesystem as a {@code opt()} parameter where
     *         the option value is {@code true}, the filesystem MAY ignore
     *         the request, or it MAY enable the feature.
     *         Any filesystem which does not support the feature, including
     *         from older releases, SHALL ignore it.
     *     </li>
     * </ol>
     */
    String FS_OPTION_CREATE_CONDITIONAL_OVERWRITE = "fs.option.create.conditional.overwrite";

    /**
     * Overwrite a file only if there is an Etag match. This option takes a string,
     *
     * Value {@value}.
     * <p>
     * This is similar to {@link #FS_OPTION_CREATE_CONDITIONAL_OVERWRITE}.
     * <ol>
     *   <li>If supported and enabled, it SHALL be declared as a capability of the filesystem</li>
     *   <li>If supported and enabled, it SHALL be declared as a capability of the stream</li>
     *   <li>The string passed as the value SHALL be the etag value as returned by
     *   {@code EtagSource.getEtag()}</li>
     *   <li>This value MUST NOT be empty</li>
     *   <li>If passed to a filesystem which supports it, then when the file is created,
     *       the store SHALL check for the existence of a file/object at the destination
     *       path.
     *   </li>
     *   <li>If there is no object there, the operation SHALL be rejected by raising
     *       either a {@code org.apache.hadoop.fs.FileAlreadyExistsException}
     *       exception, or  a{@code java.nio.file.FileAlreadyExistsException}
     *    </li>
     *   <li>If there is an object there, its Etag SHALL be compared to the
     *       value passed here.</li>
     *   <li>If there is no match, the operation SHALL be rejected by raising
     *       either a {@code org.apache.hadoop.fs.FileAlreadyExistsException}
     *       exception, or  a{@code java.nio.file.FileAlreadyExistsException}
     *    </li>
     *   <li>If the etag does match, the file SHALL be created.</li>
     *   <li>The check and create SHALL be atomic</li>
     *   <li>The check and create MAY be at the end of the write, in {@code close()},
     *       or it MAY be in the {@code create()} operation. That is: some stores
     *       MAY perform the check early</li>
     *   <li>If supported and enabled, stores MAY check for the existence of subdirectories;
     *       this behavior is implementation-specific.</li>
     * </ol>
     */
    String FS_OPTION_CREATE_CONDITIONAL_OVERWRITE_ETAG =
        "fs.option.create.conditional.overwrite.etag";

    /**
     * A flag which requires the filesystem to create files/objects in close(),
     * rather than create/createFile.
     * <p>
     * Object stores with this behavior should also export it as a path capability.
     *
     * Value {@value}.
     */
    String FS_OPTION_CREATE_IN_CLOSE = "fs.option.create.in.close";

    /**
     * String to define the content filetype.
     * Value {@value}.
     */
    String FS_OPTION_CREATE_CONTENT_TYPE = "fs.option.create.content.type";

  }
}
