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

package org.apache.hadoop.yarn.server.timelineservice.storage.flow;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.GenericConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.HBaseTimelineServerUtils;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.NumericValueConverter;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.Separator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.TimestampGenerator;
import org.apache.hadoop.yarn.server.timelineservice.storage.common.ValueConverter;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Invoked via the coprocessor when a Get or a Scan is issued for flow run
 * table. Looks through the list of cells per row, checks their tags and does
 * operation on those cells as per the cell tags. Transforms reads of the stored
 * metrics into calculated sums for each column Also, finds the min and max for
 * start and end times in a flow run.
 */
class FlowScanner implements RegionScanner, Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(FlowScanner.class);

  /**
   * use a special application id to represent the flow id this is needed since
   * TimestampGenerator parses the app id to generate a cell timestamp.
   */
  private static final String FLOW_APP_ID = "application_00000000000_0000";

  private final Region region;
  private final InternalScanner flowRunScanner;
  private final int batchSize;
  private final long appFinalValueRetentionThreshold;
  private RegionScanner regionScanner;
  private boolean hasMore;
  private byte[] currentRow;
  private List<Cell> availableCells = new ArrayList<>();
  private int currentIndex;
  private FlowScannerOperation action = FlowScannerOperation.READ;

  FlowScanner(RegionCoprocessorEnvironment env, InternalScanner internalScanner,
      FlowScannerOperation action) {
    this(env, null, internalScanner, action);
  }

  FlowScanner(RegionCoprocessorEnvironment env, Scan incomingScan,
      InternalScanner internalScanner, FlowScannerOperation action) {
    this.batchSize = incomingScan == null ? -1 : incomingScan.getBatch();
    // TODO initialize other scan attributes like Scan#maxResultSize
    this.flowRunScanner = internalScanner;
    if (internalScanner instanceof RegionScanner) {
      this.regionScanner = (RegionScanner) internalScanner;
    }
    this.action = action;
    if (env == null) {
      this.appFinalValueRetentionThreshold =
          YarnConfiguration.DEFAULT_APP_FINAL_VALUE_RETENTION_THRESHOLD;
      this.region = null;
    } else {
      this.region = env.getRegion();
      Configuration hbaseConf = env.getConfiguration();
      this.appFinalValueRetentionThreshold = hbaseConf.getLong(
          YarnConfiguration.APP_FINAL_VALUE_RETENTION_THRESHOLD,
          YarnConfiguration.DEFAULT_APP_FINAL_VALUE_RETENTION_THRESHOLD);
    }
    LOG.debug(" batch size={}", batchSize);
  }


  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.hbase.regionserver.RegionScanner#getRegionInfo()
   */
  @Override
  public HRegionInfo getRegionInfo() {
    return region.getRegionInfo();
  }

  @Override
  public boolean nextRaw(List<Cell> cells) throws IOException {
    return nextRaw(cells, ScannerContext.newBuilder().build());
  }

  @Override
  public boolean nextRaw(List<Cell> cells, ScannerContext scannerContext)
      throws IOException {
    return nextInternal(cells, scannerContext);
  }

  @Override
  public boolean next(List<Cell> cells) throws IOException {
    return next(cells, ScannerContext.newBuilder().build());
  }

  @Override
  public boolean next(List<Cell> cells, ScannerContext scannerContext)
      throws IOException {
    return nextInternal(cells, scannerContext);
  }

  /**
   * Get value converter associated with a column or a column prefix. If nothing
   * matches, generic converter is returned.
   * @param colQualifierBytes
   * @return value converter implementation.
   */
  private static ValueConverter getValueConverter(byte[] colQualifierBytes) {
    // Iterate over all the column prefixes for flow run table and get the
    // appropriate converter for the column qualifier passed if prefix matches.
    for (FlowRunColumnPrefix colPrefix : FlowRunColumnPrefix.values()) {
      byte[] colPrefixBytes = colPrefix.getColumnPrefixBytes("");
      if (Bytes.compareTo(colPrefixBytes, 0, colPrefixBytes.length,
          colQualifierBytes, 0, colPrefixBytes.length) == 0) {
        return colPrefix.getValueConverter();
      }
    }
    // Iterate over all the columns for flow run table and get the
    // appropriate converter for the column qualifier passed if match occurs.
    for (FlowRunColumn column : FlowRunColumn.values()) {
      if (Bytes.compareTo(
          column.getColumnQualifierBytes(), colQualifierBytes) == 0) {
        return column.getValueConverter();
      }
    }
    // Return generic converter if nothing matches.
    return GenericConverter.getInstance();
  }

  /**
   * This method loops through the cells in a given row of the
   * {@link FlowRunTable}. It looks at the tags of each cell to figure out how
   * to process the contents. It then calculates the sum or min or max for each
   * column or returns the cell as is.
   *
   * @param cells
   * @param scannerContext
   * @return true if next row is available for the scanner, false otherwise
   * @throws IOException
   */
  private boolean nextInternal(List<Cell> cells, ScannerContext scannerContext)
      throws IOException {
    Cell cell = null;
    startNext();
    // Loop through all the cells in this row
    // For min/max/metrics we do need to scan the entire set of cells to get the
    // right one
    // But with flush/compaction, the number of cells being scanned will go down
    // cells are grouped per column qualifier then sorted by cell timestamp
    // (latest to oldest) per column qualifier
    // So all cells in one qualifier come one after the other before we see the
    // next column qualifier
    ByteArrayComparator comp = new ByteArrayComparator();
    byte[] previousColumnQualifier = Separator.EMPTY_BYTES;
    AggregationOperation currentAggOp = null;
    SortedSet<Cell> currentColumnCells = new TreeSet<>(KeyValue.COMPARATOR);
    Set<String> alreadySeenAggDim = new HashSet<>();
    int addedCnt = 0;
    long currentTimestamp = System.currentTimeMillis();
    ValueConverter converter = null;
    int limit = batchSize;

    while (limit <= 0 || addedCnt < limit) {
      cell = peekAtNextCell(scannerContext);
      if (cell == null) {
        break;
      }
      byte[] currentColumnQualifier = CellUtil.cloneQualifier(cell);
      if (previousColumnQualifier == null) {
        // first time in loop
        previousColumnQualifier = currentColumnQualifier;
      }

      converter = getValueConverter(currentColumnQualifier);
      if (comp.compare(previousColumnQualifier, currentColumnQualifier) != 0) {
        addedCnt += emitCells(cells, currentColumnCells, currentAggOp,
            converter, currentTimestamp);
        resetState(currentColumnCells, alreadySeenAggDim);
        previousColumnQualifier = currentColumnQualifier;
        currentAggOp = getCurrentAggOp(cell);
        converter = getValueConverter(currentColumnQualifier);
      }
      collectCells(currentColumnCells, currentAggOp, cell, alreadySeenAggDim,
          converter, scannerContext);
      nextCell(scannerContext);
    }
    if ((!currentColumnCells.isEmpty()) && ((limit <= 0 || addedCnt < limit))) {
      addedCnt += emitCells(cells, currentColumnCells, currentAggOp, converter,
          currentTimestamp);
      if (LOG.isDebugEnabled()) {
        if (addedCnt > 0) {
          LOG.debug("emitted cells. " + addedCnt + " for " + this.action
              + " rowKey="
              + FlowRunRowKey.parseRowKey(CellUtil.cloneRow(cells.get(0))));
        } else {
          LOG.debug("emitted no cells for " + this.action);
        }
      }
    }
    return hasMore();
  }

  private AggregationOperation getCurrentAggOp(Cell cell) {
    List<Tag> tags = HBaseTimelineServerUtils.convertCellAsTagList(cell);
    // We assume that all the operations for a particular column are the same
    return HBaseTimelineServerUtils.getAggregationOperationFromTagsList(tags);
  }

  /**
   * resets the parameters to an initialized state for next loop iteration.
   */
  private void resetState(SortedSet<Cell> currentColumnCells,
      Set<String> alreadySeenAggDim) {
    currentColumnCells.clear();
    alreadySeenAggDim.clear();
  }

  private void collectCells(SortedSet<Cell> currentColumnCells,
      AggregationOperation currentAggOp, Cell cell,
      Set<String> alreadySeenAggDim, ValueConverter converter,
      ScannerContext scannerContext) throws IOException {

    if (currentAggOp == null) {
      // not a min/max/metric cell, so just return it as is
      currentColumnCells.add(cell);
      return;
    }

    switch (currentAggOp) {
    case GLOBAL_MIN:
      if (currentColumnCells.size() == 0) {
        currentColumnCells.add(cell);
      } else {
        Cell currentMinCell = currentColumnCells.first();
        Cell newMinCell = compareCellValues(currentMinCell, cell, currentAggOp,
            (NumericValueConverter) converter);
        if (!currentMinCell.equals(newMinCell)) {
          currentColumnCells.remove(currentMinCell);
          currentColumnCells.add(newMinCell);
        }
      }
      break;
    case GLOBAL_MAX:
      if (currentColumnCells.size() == 0) {
        currentColumnCells.add(cell);
      } else {
        Cell currentMaxCell = currentColumnCells.first();
        Cell newMaxCell = compareCellValues(currentMaxCell, cell, currentAggOp,
            (NumericValueConverter) converter);
        if (!currentMaxCell.equals(newMaxCell)) {
          currentColumnCells.remove(currentMaxCell);
          currentColumnCells.add(newMaxCell);
        }
      }
      break;
    case SUM:
    case SUM_FINAL:
      if (LOG.isTraceEnabled()) {
        LOG.trace("In collect cells "
            + " FlowSannerOperation="
            + this.action
            + " currentAggOp="
            + currentAggOp
            + " cell qualifier="
            + Bytes.toString(CellUtil.cloneQualifier(cell))
            + " cell value= "
            + converter.decodeValue(CellUtil.cloneValue(cell))
            + " timestamp=" + cell.getTimestamp());
      }

      // only if this app has not been seen yet, add to current column cells
      List<Tag> tags = HBaseTimelineServerUtils.convertCellAsTagList(cell);
      String aggDim = HBaseTimelineServerUtils
          .getAggregationCompactionDimension(tags);
      if (!alreadySeenAggDim.contains(aggDim)) {
        // if this agg dimension has already been seen,
        // since they show up in sorted order
        // we drop the rest which are older
        // in other words, this cell is older than previously seen cells
        // for that agg dim
        // but when this agg dim is not seen,
        // consider this cell in our working set
        currentColumnCells.add(cell);
        alreadySeenAggDim.add(aggDim);
      }
      break;
    default:
      break;
    } // end of switch case
  }

  /*
   * Processes the cells in input param currentColumnCells and populates
   * List<Cell> cells as the output based on the input AggregationOperation
   * parameter.
   */
  private int emitCells(List<Cell> cells, SortedSet<Cell> currentColumnCells,
      AggregationOperation currentAggOp, ValueConverter converter,
      long currentTimestamp) throws IOException {
    if ((currentColumnCells == null) || (currentColumnCells.size() == 0)) {
      return 0;
    }
    if (currentAggOp == null) {
      cells.addAll(currentColumnCells);
      return currentColumnCells.size();
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("In emitCells " + this.action + " currentColumnCells size= "
          + currentColumnCells.size() + " currentAggOp" + currentAggOp);
    }

    switch (currentAggOp) {
    case GLOBAL_MIN:
    case GLOBAL_MAX:
      cells.addAll(currentColumnCells);
      return currentColumnCells.size();
    case SUM:
    case SUM_FINAL:
      switch (action) {
      case FLUSH:
      case MINOR_COMPACTION:
        cells.addAll(currentColumnCells);
        return currentColumnCells.size();
      case READ:
        Cell sumCell = processSummation(currentColumnCells,
            (NumericValueConverter) converter);
        cells.add(sumCell);
        return 1;
      case MAJOR_COMPACTION:
        List<Cell> finalCells = processSummationMajorCompaction(
            currentColumnCells, (NumericValueConverter) converter,
            currentTimestamp);
        cells.addAll(finalCells);
        return finalCells.size();
      default:
        cells.addAll(currentColumnCells);
        return currentColumnCells.size();
      }
    default:
      cells.addAll(currentColumnCells);
      return currentColumnCells.size();
    }
  }

  /*
   * Returns a cell whose value is the sum of all cell values in the input set.
   * The new cell created has the timestamp of the most recent metric cell. The
   * sum of a metric for a flow run is the summation at the point of the last
   * metric update in that flow till that time.
   */
  private Cell processSummation(SortedSet<Cell> currentColumnCells,
      NumericValueConverter converter) throws IOException {
    Number sum = 0;
    Number currentValue = 0;
    long ts = 0L;
    long mostCurrentTimestamp = 0L;
    Cell mostRecentCell = null;
    for (Cell cell : currentColumnCells) {
      currentValue = (Number) converter.decodeValue(CellUtil.cloneValue(cell));
      ts = cell.getTimestamp();
      if (mostCurrentTimestamp < ts) {
        mostCurrentTimestamp = ts;
        mostRecentCell = cell;
      }
      sum = converter.add(sum, currentValue);
    }
    byte[] sumBytes = converter.encodeValue(sum);
    Cell sumCell =
        HBaseTimelineServerUtils.createNewCell(mostRecentCell, sumBytes);
    return sumCell;
  }


  /**
   * Returns a list of cells that contains
   *
   * A) the latest cells for applications that haven't finished yet
   * B) summation
   * for the flow, based on applications that have completed and are older than
   * a certain time
   *
   * The new cell created has the timestamp of the most recent metric cell. The
   * sum of a metric for a flow run is the summation at the point of the last
   * metric update in that flow till that time.
   */
  @VisibleForTesting
  List<Cell> processSummationMajorCompaction(
      SortedSet<Cell> currentColumnCells, NumericValueConverter converter,
      long currentTimestamp)
      throws IOException {
    Number sum = 0;
    Number currentValue = 0;
    long ts = 0L;
    boolean summationDone = false;
    List<Cell> finalCells = new ArrayList<Cell>();
    if (currentColumnCells == null) {
      return finalCells;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("In processSummationMajorCompaction,"
          + " will drop cells older than " + currentTimestamp
          + " CurrentColumnCells size=" + currentColumnCells.size());
    }

    for (Cell cell : currentColumnCells) {
      AggregationOperation cellAggOp = getCurrentAggOp(cell);
      // if this is the existing flow sum cell
      List<Tag> tags = HBaseTimelineServerUtils.convertCellAsTagList(cell);
      String appId = HBaseTimelineServerUtils
          .getAggregationCompactionDimension(tags);
      if (appId == FLOW_APP_ID) {
        sum = converter.add(sum, currentValue);
        summationDone = true;
        if (LOG.isTraceEnabled()) {
          LOG.trace("reading flow app id sum=" + sum);
        }
      } else {
        currentValue = (Number) converter.decodeValue(CellUtil
            .cloneValue(cell));
        // read the timestamp truncated by the generator
        ts =  TimestampGenerator.getTruncatedTimestamp(cell.getTimestamp());
        if ((cellAggOp == AggregationOperation.SUM_FINAL)
            && ((ts + this.appFinalValueRetentionThreshold)
                < currentTimestamp)) {
          sum = converter.add(sum, currentValue);
          summationDone = true;
          if (LOG.isTraceEnabled()) {
            LOG.trace("MAJOR COMPACTION loop sum= " + sum
                + " discarding now: " + " qualifier="
                + Bytes.toString(CellUtil.cloneQualifier(cell)) + " value="
                + converter.decodeValue(CellUtil.cloneValue(cell))
                + " timestamp=" + cell.getTimestamp() + " " + this.action);
          }
        } else {
          // not a final value but it's the latest cell for this app
          // so include this cell in the list of cells to write back
          finalCells.add(cell);
        }
      }
    }
    if (summationDone) {
      Cell anyCell = currentColumnCells.first();
      List<Tag> tags = new ArrayList<Tag>();
      Tag t = HBaseTimelineServerUtils.createTag(
          AggregationOperation.SUM_FINAL.getTagType(),
          Bytes.toBytes(FLOW_APP_ID));
      tags.add(t);
      t = HBaseTimelineServerUtils.createTag(
          AggregationCompactionDimension.APPLICATION_ID.getTagType(),
          Bytes.toBytes(FLOW_APP_ID));
      tags.add(t);
      byte[] tagByteArray =
          HBaseTimelineServerUtils.convertTagListToByteArray(tags);
      Cell sumCell = HBaseTimelineServerUtils.createNewCell(
          CellUtil.cloneRow(anyCell),
          CellUtil.cloneFamily(anyCell),
          CellUtil.cloneQualifier(anyCell),
          TimestampGenerator.getSupplementedTimestamp(
              System.currentTimeMillis(), FLOW_APP_ID),
              converter.encodeValue(sum), tagByteArray);
      finalCells.add(sumCell);
      if (LOG.isTraceEnabled()) {
        LOG.trace("MAJOR COMPACTION final sum= " + sum + " for "
            + Bytes.toString(CellUtil.cloneQualifier(sumCell))
            + " " + this.action);
      }
      LOG.info("After major compaction for qualifier="
          + Bytes.toString(CellUtil.cloneQualifier(sumCell))
          + " with currentColumnCells.size="
          + currentColumnCells.size()
          + " returning finalCells.size=" + finalCells.size()
          + " with sum=" + sum.longValue()
          + " with cell timestamp " + sumCell.getTimestamp());
    } else {
      String qualifier = "";
      LOG.info("After major compaction for qualifier=" + qualifier
          + " with currentColumnCells.size="
          + currentColumnCells.size()
          + " returning finalCells.size=" + finalCells.size()
          + " with zero sum="
          + sum.longValue());
    }
    return finalCells;
  }

  /**
   * Determines which cell is to be returned based on the values in each cell
   * and the comparison operation MIN or MAX.
   *
   * @param previouslyChosenCell
   * @param currentCell
   * @param currentAggOp
   * @return the cell which is the min (or max) cell
   * @throws IOException
   */
  private Cell compareCellValues(Cell previouslyChosenCell, Cell currentCell,
      AggregationOperation currentAggOp, NumericValueConverter converter)
      throws IOException {
    if (previouslyChosenCell == null) {
      return currentCell;
    }
    try {
      Number previouslyChosenCellValue = (Number)converter.decodeValue(
          CellUtil.cloneValue(previouslyChosenCell));
      Number currentCellValue = (Number) converter.decodeValue(CellUtil
          .cloneValue(currentCell));
      switch (currentAggOp) {
      case GLOBAL_MIN:
        if (converter.compare(
            currentCellValue, previouslyChosenCellValue) < 0) {
          // new value is minimum, hence return this cell
          return currentCell;
        } else {
          // previously chosen value is miniumum, hence return previous min cell
          return previouslyChosenCell;
        }
      case GLOBAL_MAX:
        if (converter.compare(
            currentCellValue, previouslyChosenCellValue) > 0) {
          // new value is max, hence return this cell
          return currentCell;
        } else {
          // previously chosen value is max, hence return previous max cell
          return previouslyChosenCell;
        }
      default:
        return currentCell;
      }
    } catch (IllegalArgumentException iae) {
      LOG.error("caught iae during conversion to long ", iae);
      return currentCell;
    }
  }

  @Override
  public void close() throws IOException {
    if (flowRunScanner != null) {
      flowRunScanner.close();
    } else {
      LOG.warn("scanner close called but scanner is null");
    }
  }

  /**
   * Called to signal the start of the next() call by the scanner.
   */
  public void startNext() {
    currentRow = null;
  }

  /**
   * Returns whether or not the underlying scanner has more rows.
   */
  public boolean hasMore() {
    return currentIndex < availableCells.size() ? true : hasMore;
  }

  /**
   * Returns the next available cell for the current row and advances the
   * pointer to the next cell. This method can be called multiple times in a row
   * to advance through all the available cells.
   *
   * @param scannerContext
   *          context information for the batch of cells under consideration
   * @return the next available cell or null if no more cells are available for
   *         the current row
   * @throws IOException
   */
  public Cell nextCell(ScannerContext scannerContext) throws IOException {
    Cell cell = peekAtNextCell(scannerContext);
    if (cell != null) {
      currentIndex++;
    }
    return cell;
  }

  /**
   * Returns the next available cell for the current row, without advancing the
   * pointer. Calling this method multiple times in a row will continue to
   * return the same cell.
   *
   * @param scannerContext
   *          context information for the batch of cells under consideration
   * @return the next available cell or null if no more cells are available for
   *         the current row
   * @throws IOException if any problem is encountered while grabbing the next
   *     cell.
   */
  public Cell peekAtNextCell(ScannerContext scannerContext) throws IOException {
    if (currentIndex >= availableCells.size()) {
      // done with current batch
      availableCells.clear();
      currentIndex = 0;
      hasMore = flowRunScanner.next(availableCells, scannerContext);
    }
    Cell cell = null;
    if (currentIndex < availableCells.size()) {
      cell = availableCells.get(currentIndex);
      if (currentRow == null) {
        currentRow = CellUtil.cloneRow(cell);
      } else if (!CellUtil.matchingRow(cell, currentRow)) {
        // moved on to the next row
        // don't use the current cell
        // also signal no more cells for this row
        return null;
      }
    }
    return cell;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.hbase.regionserver.RegionScanner#getMaxResultSize()
   */
  @Override
  public long getMaxResultSize() {
    if (regionScanner == null) {
      throw new IllegalStateException(
          "RegionScanner.isFilterDone() called when the flow "
              + "scanner's scanner is not a RegionScanner");
    }
    return regionScanner.getMaxResultSize();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.hbase.regionserver.RegionScanner#getMvccReadPoint()
   */
  @Override
  public long getMvccReadPoint() {
    if (regionScanner == null) {
      throw new IllegalStateException(
          "RegionScanner.isFilterDone() called when the flow "
              + "scanner's internal scanner is not a RegionScanner");
    }
    return regionScanner.getMvccReadPoint();
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.hbase.regionserver.RegionScanner#isFilterDone()
   */
  @Override
  public boolean isFilterDone() throws IOException {
    if (regionScanner == null) {
      throw new IllegalStateException(
          "RegionScanner.isFilterDone() called when the flow "
              + "scanner's internal scanner is not a RegionScanner");
    }
    return regionScanner.isFilterDone();

  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.hadoop.hbase.regionserver.RegionScanner#reseek(byte[])
   */
  @Override
  public boolean reseek(byte[] bytes) throws IOException {
    if (regionScanner == null) {
      throw new IllegalStateException(
          "RegionScanner.reseek() called when the flow "
              + "scanner's internal scanner is not a RegionScanner");
    }
    return regionScanner.reseek(bytes);
  }

  @Override
  public int getBatch() {
    return batchSize;
  }
}
