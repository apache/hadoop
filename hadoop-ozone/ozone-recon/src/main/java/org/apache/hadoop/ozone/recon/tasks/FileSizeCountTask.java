/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.tasks;

import com.google.inject.Inject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.utils.db.Table;
import org.apache.hadoop.utils.db.TableIterator;
import org.hadoop.ozone.recon.schema.tables.daos.FileCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.pojos.FileCountBySize;
import org.jooq.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.apache.hadoop.ozone.recon.tasks.
    OMDBUpdateEvent.OMDBUpdateAction.DELETE;
import static org.apache.hadoop.ozone.recon.tasks.
    OMDBUpdateEvent.OMDBUpdateAction.PUT;

/**
 * Class to iterate over the OM DB and store the counts of existing/new
 * files binned into ranges (1KB, 2Kb..,4MB,.., 1TB,..1PB) to the Recon
 * fileSize DB.
 */
public class FileSizeCountTask extends ReconDBUpdateTask {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSizeCountTask.class);

  private int maxBinSize = -1;
  private long maxFileSizeUpperBound = 1125899906842624L; // 1 PB
  private long[] upperBoundCount;
  private long oneKb = 1024L;
  private Collection<String> tables = new ArrayList<>();
  private FileCountBySizeDao fileCountBySizeDao;

  @Inject
  public FileSizeCountTask(OMMetadataManager omMetadataManager,
      Configuration sqlConfiguration) {
    super("FileSizeCountTask");
    try {
      tables.add(omMetadataManager.getKeyTable().getName());
      fileCountBySizeDao = new FileCountBySizeDao(sqlConfiguration);
    } catch (Exception e) {
      LOG.error("Unable to fetch Key Table updates ", e);
    }
    upperBoundCount = new long[getMaxBinSize()];
  }

  long getOneKB() {
    return oneKb;
  }

  long getMaxFileSizeUpperBound() {
    return maxFileSizeUpperBound;
  }

  int getMaxBinSize() {
    if (maxBinSize == -1) {
      // extra bin to add files > 1PB.
      // 1 KB (2 ^ 10) is the smallest tracked file.
      maxBinSize = nextClosestPowerIndexOfTwo(maxFileSizeUpperBound) - 10 + 1;
    }
    return maxBinSize;
  }

  /**
   * Read the Keys from OM snapshot DB and calculate the upper bound of
   * File Size it belongs to.
   *
   * @param omMetadataManager OM Metadata instance.
   * @return Pair
   */
  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    LOG.info("Starting a 'reprocess' run of FileSizeCountTask.");
    Table<String, OmKeyInfo> omKeyInfoTable = omMetadataManager.getKeyTable();
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        keyIter = omKeyInfoTable.iterator()) {
      while (keyIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();

        // reprocess() is a PUT operation on the DB.
        updateUpperBoundCount(kv.getValue(), PUT);
      }
    } catch (IOException ioEx) {
      LOG.error("Unable to populate File Size Count in Recon DB. ", ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }
    populateFileCountBySizeDB();

    LOG.info("Completed a 'reprocess' run of FileSizeCountTask.");
    return new ImmutablePair<>(getTaskName(), true);
  }

  @Override
  protected Collection<String> getTaskTables() {
    return tables;
  }

  private void updateCountFromDB() {
    // Read - Write operations to DB are in ascending order
    // of file size upper bounds.
    List<FileCountBySize> resultSet = fileCountBySizeDao.findAll();
    int index = 0;
    if (resultSet != null) {
      for (FileCountBySize row : resultSet) {
        upperBoundCount[index] = row.getCount();
        index++;
      }
    }
  }

  /**
   * Read the Keys from update events and update the count of files
   * pertaining to a certain upper bound.
   *
   * @param events Update events - PUT/DELETE.
   * @return Pair
   */
  @Override
  Pair<String, Boolean> process(OMUpdateEventBatch events) {
    LOG.info("Starting a 'process' run of FileSizeCountTask.");
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();

    //update array with file size count from DB
    updateCountFromDB();

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, OmKeyInfo> omdbUpdateEvent = eventIterator.next();
      String updatedKey = omdbUpdateEvent.getKey();
      OmKeyInfo omKeyInfo = omdbUpdateEvent.getValue();

      try{
        switch (omdbUpdateEvent.getAction()) {
        case PUT:
          updateUpperBoundCount(omKeyInfo, PUT);
          break;

        case DELETE:
          updateUpperBoundCount(omKeyInfo, DELETE);
          break;

        default: LOG.trace("Skipping DB update event : " + omdbUpdateEvent
                  .getAction());
        }
      } catch (IOException e) {
        LOG.error("Unexpected exception while updating key data : {} {}",
                updatedKey, e.getMessage());
        return new ImmutablePair<>(getTaskName(), false);
      }
      populateFileCountBySizeDB();
    }
    LOG.info("Completed a 'process' run of FileSizeCountTask.");
    return new ImmutablePair<>(getTaskName(), true);
  }

  /**
   * Calculate the bin index based on size of the Key.
   * index is calculated as the number of right shifts
   * needed until dataSize becomes zero.
   *
   * @param dataSize Size of the key.
   * @return int bin index in upperBoundCount
   */
  public int calculateBinIndex(long dataSize) {
    if (dataSize >= getMaxFileSizeUpperBound()) {
      return getMaxBinSize() - 1;
    }
    int index = nextClosestPowerIndexOfTwo(dataSize);
    // The smallest file size being tracked for count
    // is 1 KB i.e. 1024 = 2 ^ 10.
    return index < 10 ? 0 : index - 10;
  }

  int nextClosestPowerIndexOfTwo(long dataSize) {
    int index = 0;
    while(dataSize != 0) {
      dataSize >>= 1;
      index += 1;
    }
    return index;
  }

  /**
   * Populate DB with the counts of file sizes calculated
   * using the dao.
   *
   */
  void populateFileCountBySizeDB() {
    for (int i = 0; i < upperBoundCount.length; i++) {
      long fileSizeUpperBound = (i == upperBoundCount.length - 1) ?
          Long.MAX_VALUE : (long) Math.pow(2, (10 + i));
      FileCountBySize fileCountRecord =
          fileCountBySizeDao.findById(fileSizeUpperBound);
      FileCountBySize newRecord = new
          FileCountBySize(fileSizeUpperBound, upperBoundCount[i]);
      if (fileCountRecord == null) {
        fileCountBySizeDao.insert(newRecord);
      } else {
        fileCountBySizeDao.update(newRecord);
      }
    }
  }

  /**
   * Calculate and update the count of files being tracked by
   * upperBoundCount[].
   * Used by reprocess() and process().
   *
   * @param omKeyInfo OmKey being updated for count
   * @param operation (PUT, DELETE)
   */
  void updateUpperBoundCount(OmKeyInfo omKeyInfo,
      OMDBUpdateEvent.OMDBUpdateAction operation) throws IOException {
    int binIndex = calculateBinIndex(omKeyInfo.getDataSize());
    if (operation == PUT) {
      upperBoundCount[binIndex]++;
    } else if (operation == DELETE) {
      if (upperBoundCount[binIndex] != 0) {
        //decrement only if it had files before, default DB value is 0
        upperBoundCount[binIndex]--;
      } else {
        LOG.debug("Cannot decrement count. Default value is 0 (zero).");
        throw new IOException("Cannot decrement count. "
            + "Default value is 0 (zero).");
      }
    }
  }
}
