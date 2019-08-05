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

/**
 * Class to iterate over the OM DB and store the counts of existing/new
 * files binned into ranges (1KB, 10Kb..,10MB,..1PB) to the Recon
 * fileSize DB.
 */
public class FileSizeCountTask extends ReconDBUpdateTask {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSizeCountTask.class);

  private int maxBinSize;
  private long maxFileSizeUpperBound = 1125899906842624L; // 1 PB
  private long[] upperBoundCount = new long[maxBinSize];
  private long ONE_KB = 1024L;
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
  }

  protected long getOneKB() {
    return ONE_KB;
  }

  protected long getMaxFileSizeUpperBound() {
    return maxFileSizeUpperBound;
  }

  protected int getMaxBinSize() {
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

    fetchUpperBoundCount("reprocess");

    Table<String, OmKeyInfo> omKeyInfoTable = omMetadataManager.getKeyTable();
    try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
        keyIter = omKeyInfoTable.iterator()) {
      while (keyIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyIter.next();
        countFileSize(kv.getValue());
      }
    } catch (IOException ioEx) {
      LOG.error("Unable to populate File Size Count in Recon DB. ", ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    } finally {
      populateFileCountBySizeDB();
    }

    LOG.info("Completed a 'reprocess' run of FileSizeCountTask.");
    return new ImmutablePair<>(getTaskName(), true);
  }

  void setMaxBinSize() {
    maxBinSize = (int)(long) (Math.log(getMaxFileSizeUpperBound())
        /Math.log(2)) - 10;
    maxBinSize += 2;  // extra bin to add files > 1PB.
  }

  void fetchUpperBoundCount(String type) {
    setMaxBinSize();
    if (type.equals("process")) {
      //update array with file size count from DB
      List<FileCountBySize> resultSet = fileCountBySizeDao.findAll();
      int index = 0;
      if (resultSet != null) {
        for (FileCountBySize row : resultSet) {
          upperBoundCount[index] = row.getCount();
          index++;
        }
      }
    } else {
      upperBoundCount = new long[getMaxBinSize()];    //initialize array
    }
  }

  @Override
  protected Collection<String> getTaskTables() {
    return tables;
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

    fetchUpperBoundCount("process");

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, OmKeyInfo> omdbUpdateEvent = eventIterator.next();
      String updatedKey = omdbUpdateEvent.getKey();
      OmKeyInfo updatedValue = omdbUpdateEvent.getValue();

      try{
        switch (omdbUpdateEvent.getAction()) {
        case PUT:
          updateUpperBoundCount(updatedValue, "PUT");
          break;

        case DELETE:
          updateUpperBoundCount(updatedValue, "DELETE");
          break;

        default: LOG.trace("Skipping DB update event : " + omdbUpdateEvent
                  .getAction());
        }
      } catch (IOException e) {
        LOG.error("Unexpected exception while updating key data : {} {}",
                updatedKey, e.getMessage());
        return new ImmutablePair<>(getTaskName(), false);
      } finally {
        populateFileCountBySizeDB();
      }
    }
    LOG.info("Completed a 'process' run of FileSizeCountTask.");
    return new ImmutablePair<>(getTaskName(), true);
  }

  /**
   * Calculate the bin index based on size of the Key.
   * The logic is works by setting all bits after the
   * leftmost set bit in (n-1).
   *
   * @param dataSize Size of the key.
   * @return int bin index in upperBoundCount
   */
  int calculateBinIndex(long dataSize) {
    // files >= 1PB go into the last bin.
    if (dataSize >= getMaxFileSizeUpperBound()) {
      return getMaxBinSize() - 1;
    }
    int index = 0;
    if (dataSize % getOneKB() == 0) {
      index = (int) (long) (Math.log(dataSize)/Math.log(2)) + 1;
    } else {
      dataSize--;
      dataSize |= dataSize >> 1;
      dataSize |= dataSize >> 2;
      dataSize |= dataSize >> 4;
      dataSize |= dataSize >> 8;
      dataSize |= dataSize >> 16;
      dataSize |= dataSize >> 32;
      dataSize++;

      index = (int) (long) (Math.log(dataSize)/Math.log(2));
    }
    return index < 10 ? 0 : index - 10;
  }

  void countFileSize(OmKeyInfo omKeyInfo) {
    int index = calculateBinIndex(omKeyInfo.getDataSize());
    upperBoundCount[index]++;
  }

  void populateFileCountBySizeDB() {
    for (int i = 0; i < upperBoundCount.length; i++) {
      long fileSizeUpperBound = (long) Math.pow(2, (10 + i));
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

  private void updateUpperBoundCount(OmKeyInfo value, String operation)
      throws IOException {
    int binIndex = calculateBinIndex(value.getDataSize());
    if (binIndex == Integer.MIN_VALUE) {
      throw new IOException("File Size larger than permissible file size");
    }
    if (operation.equals("PUT")) {
      upperBoundCount[binIndex]++;
    } else if (operation.equals("DELETE")) {
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
