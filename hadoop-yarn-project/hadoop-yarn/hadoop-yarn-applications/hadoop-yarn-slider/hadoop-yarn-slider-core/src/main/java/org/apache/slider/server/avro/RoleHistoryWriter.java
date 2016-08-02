/*
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

package org.apache.slider.server.avro;

import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.server.appmaster.state.NodeEntry;
import org.apache.slider.server.appmaster.state.NodeInstance;
import org.apache.slider.server.appmaster.state.RoleHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;

/**
 * Write out the role history to an output stream.
 */
public class RoleHistoryWriter {
  protected static final Logger log =
    LoggerFactory.getLogger(RoleHistoryWriter.class);

  /**
   * Although Avro is designed to handle some changes, we still keep a version
   * marker in the file to catch changes that are fundamentally incompatible
   * at the semantic level -changes that require either a different
   * parser or get rejected outright.
   */
  public static final int ROLE_HISTORY_VERSION = 0x01;
  
  /**
   * Write out the history.
   * This does not update the history's dirty/savetime fields
   *
   * @param out outstream
   * @param history history
   * @param savetime time in millis for the save time to go in as a record
   * @return no of records written
   * @throws IOException IO failures
   */
  public long write(OutputStream out, RoleHistory history, long savetime)
    throws IOException {
    try {
      DatumWriter<RoleHistoryRecord> writer =
        new SpecificDatumWriter<>(RoleHistoryRecord.class);

      RoleHistoryRecord record = createHeaderRecord(savetime, history);
      int roles = history.getRoleSize();
      Schema schema = record.getSchema();
      Encoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
      writer.write(record, encoder);
      // now write the rolemap record
      writer.write(createRolemapRecord(history), encoder);
      long count = 0;
      //now for every role history entry, write out its record
      Collection<NodeInstance> instances = history.cloneNodemap().values();
      for (NodeInstance instance : instances) {
        for (int role = 0; role < roles; role++) {
          NodeEntry nodeEntry = instance.get(role);

          if (nodeEntry != null) {
            NodeEntryRecord ner = build(nodeEntry, role, instance.hostname);
            record = new RoleHistoryRecord(ner);
            writer.write(record, encoder);
            count++;
          }
        }
      }
      // footer
      RoleHistoryFooter footer = new RoleHistoryFooter();
      footer.setCount(count);
      writer.write(new RoleHistoryRecord(footer), encoder);
      encoder.flush();
      return count;
    } finally {
      out.close();
    }
  }

  /**
   * Create the header record
   * @param savetime time of save
   * @param history history
   * @return a record to place at the head of the file
   */
  private RoleHistoryRecord createHeaderRecord(long savetime, RoleHistory history) {
    RoleHistoryHeader header = new RoleHistoryHeader();
    header.setVersion(ROLE_HISTORY_VERSION);
    header.setSaved(savetime);
    header.setSavedx(Long.toHexString(savetime));
    header.setSavedate(SliderUtils.toGMTString(savetime));
    header.setRoles(history.getRoleSize());
    return new RoleHistoryRecord(header);
  }

  /**
   * Create the rolemap record
   * @param history history
   * @return a record to insert into the file
   */
  private RoleHistoryRecord createRolemapRecord(RoleHistory history) {
    RoleHistoryMapping entry = new RoleHistoryMapping();
    Map<CharSequence, Integer> mapping = history.buildMappingForHistoryFile();
    entry.setRolemap(mapping);
    return new RoleHistoryRecord(entry);
  }

  /**
   * Write the history information to a file
   *
   * @param fs filesystem
   * @param path path
   * @param overwrite overwrite flag
   * @param history history
   * @param savetime time in millis for the save time to go in as a record
   * @return no of records written
   * @throws IOException IO failures
   */
  public long write(FileSystem fs,
      Path path,
      boolean overwrite,
      RoleHistory history,
      long savetime)
      throws IOException {
    FSDataOutputStream out = fs.create(path, overwrite);
    return write(out, history, savetime);
  }


  /**
   * Create the filename for a history file
   * @param time time value
   * @return a filename such that later filenames sort later in the directory
   */
  public Path createHistoryFilename(Path historyPath, long time) {
    String filename = String.format(Locale.ENGLISH,
                                    SliderKeys.HISTORY_FILENAME_CREATION_PATTERN,
                                    time);
    Path path = new Path(historyPath, filename);
    return path;
  }

  /**
   * Build a {@link NodeEntryRecord} from a node entry; include whether
   * the node is in use and when it was last used.
   * @param entry entry count
   * @param role role index
   * @param hostname name
   * @return the record
   */
  private NodeEntryRecord build(NodeEntry entry, int role, String hostname) {
    NodeEntryRecord record = new NodeEntryRecord(
      hostname, role, entry.getLive() > 0, entry.getLastUsed()
    );
    return record;
  }

  /**
   * Read a history, returning one that is ready to have its onThaw() 
   * method called
   * @param in input source
   * @return no. of entries read
   * @throws IOException problems
   */
  public LoadedRoleHistory read(InputStream in) throws
                                                       IOException,
                                                       BadConfigException {
    try {
      LoadedRoleHistory loadedRoleHistory = new LoadedRoleHistory();
      DatumReader<RoleHistoryRecord> reader =
        new SpecificDatumReader<>(RoleHistoryRecord.class);
      Decoder decoder =
        DecoderFactory.get().jsonDecoder(RoleHistoryRecord.getClassSchema(),
            in);

      //read header : no entry -> EOF
      RoleHistoryRecord record = reader.read(null, decoder);
      if (record == null) {
        throw new IOException("Role History Header not found at start of file.");
      }
      Object entry = record.getEntry();
      if (!(entry instanceof RoleHistoryHeader)) {
        throw new IOException("Role History Header not found at start of file");
      }
      RoleHistoryHeader header = (RoleHistoryHeader) entry;
      if (header.getVersion() != ROLE_HISTORY_VERSION) {
        throw new IOException(
          String.format("Can't read role file version %04x -need %04x",
          header.getVersion(),
          ROLE_HISTORY_VERSION));
      }
      loadedRoleHistory.setHeader(header);
      RoleHistoryFooter footer = null;
      int records = 0;
      //go through reading data
      try {
        while (footer == null) {
          record = reader.read(null, decoder);
          if (record == null) {
            throw new IOException("Null record after " + records + " records");
          }
          entry = record.getEntry();

          if (entry instanceof RoleHistoryHeader) {
            throw new IOException("Duplicate Role History Header found");
          } else if (entry instanceof RoleHistoryMapping) {
            // role history mapping entry
            if (!loadedRoleHistory.roleMap.isEmpty()) {
              // duplicate role maps are viewed as something to warn over, rather than fail
              log.warn("Duplicate role map; ignoring");
            } else {
              RoleHistoryMapping historyMapping = (RoleHistoryMapping) entry;
              loadedRoleHistory.buildMapping(historyMapping.getRolemap());
            }
          } else if (entry instanceof NodeEntryRecord) {
            // normal record
            records++;
            NodeEntryRecord nodeEntryRecord = (NodeEntryRecord) entry;
            loadedRoleHistory.add(nodeEntryRecord);
          } else if (entry instanceof RoleHistoryFooter) {
            //tail end of the file
            footer = (RoleHistoryFooter) entry;
          } else {
            // this is to handle future versions, such as when rolling back
            // from a later version of slider
            log.warn("Discarding unknown record {}", entry);
          }
        }
      } catch (EOFException e) {
        EOFException ex = new EOFException(
          "End of file reached after " + records + " records");
        ex.initCause(e);
        throw ex;
      }
      // at this point there should be no data left.
      // check by reading and expecting a -1
      if (in.read() > 0) {
        // footer is in stream before the last record
        throw new EOFException(
          "File footer reached before end of file -after " + records +
          " records");
      }
      if (records != footer.getCount()) {
        log.warn("mismatch between no of records saved {} and number read {}",
                 footer.getCount(), records);
      }
      return loadedRoleHistory;
    } finally {
      in.close();
    }

  }

  /**
   * Read a role history from a path in a filesystem
   * @param fs filesystem
   * @param path path to the file
   * @return the records read
   * @throws IOException any problem
   */
  public LoadedRoleHistory read(FileSystem fs, Path path)
      throws IOException, BadConfigException {
    FSDataInputStream instream = fs.open(path);
    return read(instream);
  }

  /**
   * Read from a resource in the classpath -used for testing
   * @param resource resource
   * @return the records read
   * @throws IOException any problem
   */
  public LoadedRoleHistory read(String resource)
      throws IOException, BadConfigException {

    return read(this.getClass().getClassLoader().getResourceAsStream(resource));
  }


  /**
   * Find all history entries in a dir. The dir is created if it is
   * not already defined.
   * 
   * The scan uses the match pattern {@link SliderKeys#HISTORY_FILENAME_MATCH_PATTERN}
   * while dropping empty files and directories which match the pattern.
   * The list is then sorted with a comparator that sorts on filename,
   * relying on the filename of newer created files being later than the old ones.
   * 
   * 
   *
   * @param fs filesystem
   * @param dir dir to scan
   * @param includeEmptyFiles should empty files be included in the result?
   * @return a possibly empty list
   * @throws IOException IO problems
   * @throws FileNotFoundException if the target dir is actually a path
   */
  public List<Path> findAllHistoryEntries(FileSystem fs,
                                          Path dir,
                                          boolean includeEmptyFiles) throws IOException {
    assert fs != null;
    assert dir != null;
    if (!fs.exists(dir)) {
      fs.mkdirs(dir);
    } else if (!fs.isDirectory(dir)) {
      throw new FileNotFoundException("Not a directory " + dir.toString());
    }
    
    PathFilter filter = new GlobFilter(SliderKeys.HISTORY_FILENAME_GLOB_PATTERN);
    FileStatus[] stats = fs.listStatus(dir, filter);
    List<Path> paths = new ArrayList<Path>(stats.length);
    for (FileStatus stat : stats) {
      log.debug("Possible entry: {}", stat.toString());
      if (stat.isFile() && (includeEmptyFiles || stat.getLen() > 0)) {
        paths.add(stat.getPath());
      }
    }
    sortHistoryPaths(paths);
    return paths;
  }

  @VisibleForTesting
  public static void sortHistoryPaths(List<Path> paths) {
    Collections.sort(paths, new NewerFilesFirst());
  }
  
  /**
   * Iterate through the paths until one can be loaded
   * @param paths paths to load
   * @return the loaded history including the path -or null if all failed to load
   */
  public LoadedRoleHistory attemptToReadHistory(FileSystem fileSystem,
      List<Path> paths)
      throws BadConfigException {
    ListIterator<Path> pathIterator = paths.listIterator();
    boolean success = false;
    LoadedRoleHistory history = null;
    while (!success && pathIterator.hasNext()) {
      Path path = pathIterator.next();
      try {
        history = read(fileSystem, path);
        //success
        success = true;
        history.setPath(path);
      } catch (IOException e) {
        log.info("Failed to read {}", path, e);
      } catch (AvroTypeException e) {
        log.warn("Failed to parse {}", path, e);
      } catch (Exception e) {
        // low level event logged @ warn level
        log.warn("Exception while reading {}", path, e);
      }
    }
    return history;
  }

  /**
   * Try to load the history from a directory -a failure to load a specific
   * file is downgraded to a log and the next older path attempted instead
   * @param fs filesystem
   * @param dir dir to load from
   * @return the history loaded, including the path
   * @throws IOException if indexing the history directory fails. 
   */
  public LoadedRoleHistory loadFromHistoryDir(FileSystem fs, Path dir)
      throws IOException, BadConfigException {
    assert fs != null: "null filesystem";
    List<Path> entries = findAllHistoryEntries(fs, dir, false);
    return attemptToReadHistory(fs, entries);
  }

  /**
   * Delete all old history entries older than the one we want to keep. This
   * uses the filename ordering to determine age, not timestamps
   * @param fileSystem filesystem
   * @param keep path to keep -used in thresholding the files
   * @return the number of files deleted
   * @throws FileNotFoundException if the path to keep is not present (safety
   * check to stop the entire dir being purged)
   * @throws IOException IO problems
   */
  public int purgeOlderHistoryEntries(FileSystem fileSystem, Path keep)
      throws IOException { assert fileSystem != null : "null filesystem";
    if (!fileSystem.exists(keep)) {
      throw new FileNotFoundException(keep.toString());
    }
    Path dir = keep.getParent();
    log.debug("Purging entries in {} up to {}", dir, keep);
    List<Path> paths = findAllHistoryEntries(fileSystem, dir, true);
    Collections.sort(paths, new OlderFilesFirst());
    int deleteCount = 0;
    for (Path path : paths) {
      if (path.equals(keep)) {
        break;
      } else {
        log.debug("Deleting {}", path);
        deleteCount++;
        fileSystem.delete(path, false);
      }
    }
    return deleteCount;
  }

}
