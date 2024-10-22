/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.timelineservice.storage.common;

import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationCompactionDimension;
import org.apache.hadoop.yarn.server.timelineservice.storage.flow.AggregationOperation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A utility class used by hbase-server module.
 */
public final class HBaseTimelineServerUtils {
  private HBaseTimelineServerUtils() {
  }

  /**
   * Creates a {@link Tag} from the input attribute.
   *
   * @param attribute Attribute from which tag has to be fetched.
   * @return a HBase Tag.
   */
  public static Tag getTagFromAttribute(Map.Entry<String, byte[]> attribute) {
    // attribute could be either an Aggregation Operation or
    // an Aggregation Dimension
    // Get the Tag type from either
    AggregationOperation aggOp = AggregationOperation
        .getAggregationOperation(attribute.getKey());
    if (aggOp != null) {
      Tag t = createTag(aggOp.getTagType(), attribute.getValue());
      return t;
    }

    AggregationCompactionDimension aggCompactDim =
        AggregationCompactionDimension.getAggregationCompactionDimension(
            attribute.getKey());
    if (aggCompactDim != null) {
      Tag t = createTag(aggCompactDim.getTagType(), attribute.getValue());
      return t;
    }
    return null;
  }

  /**
   * creates a new cell based on the input cell but with the new value.
   *
   * @param origCell Original cell
   * @param newValue new cell value
   * @return cell
   * @throws IOException while creating new cell.
   */
  public static Cell createNewCell(Cell origCell, byte[] newValue)
      throws IOException {
    return CellUtil.createCell(CellUtil.cloneRow(origCell),
        CellUtil.cloneFamily(origCell), CellUtil.cloneQualifier(origCell),
        origCell.getTimestamp(), KeyValue.Type.Put.getCode(), newValue);
  }

  /**
   * creates a cell with the given inputs.
   *
   * @param row row of the cell to be created
   * @param family column family name of the new cell
   * @param qualifier qualifier for the new cell
   * @param ts timestamp of the new cell
   * @param newValue value of the new cell
   * @param tags tags in the new cell
   * @return cell
   * @throws IOException while creating the cell.
   */
  public static Cell createNewCell(byte[] row, byte[] family, byte[] qualifier,
      long ts, byte[] newValue, byte[] tags) throws IOException {
    return CellUtil.createCell(row, family, qualifier, ts, KeyValue.Type.Put,
        newValue, tags);
  }

  /**
   * Create a Tag.
   * @param tagType tag type
   * @param tag the content of the tag in byte array.
   * @return an instance of Tag
   */
  public static Tag createTag(byte tagType, byte[] tag) {
    return new ArrayBackedTag(tagType, tag);
  }

  /**
   * Create a Tag.
   * @param tagType tag type
   * @param tag the content of the tag in String.
   * @return an instance of Tag
   */
  public static Tag createTag(byte tagType, String tag) {
    return createTag(tagType, Bytes.toBytes(tag));
  }

  /**
   * Convert a cell to a list of tags.
   * @param cell the cell to convert
   * @return a list of tags
   */
  public static List<Tag> convertCellAsTagList(Cell cell) {
    return TagUtil.asList(
        cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
  }

  /**
   * Convert a list of tags to a byte array.
   * @param tags the list of tags to convert
   * @return byte array representation of the list of tags
   */
  public static byte[] convertTagListToByteArray(List<Tag> tags) {
    return TagUtil.fromList(tags);
  }

  /**
   * returns app id from the list of tags.
   *
   * @param tags cell tags to be looked into
   * @return App Id as the AggregationCompactionDimension
   */
  public static String getAggregationCompactionDimension(List<Tag> tags) {
    String appId = null;
    for (Tag t : tags) {
      if (AggregationCompactionDimension.APPLICATION_ID.getTagType() == t
          .getType()) {
        appId = Bytes.toString(Tag.cloneValue(t));
        return appId;
      }
    }
    return appId;
  }

  /**
   * Returns the first seen aggregation operation as seen in the list of input
   * tags or null otherwise.
   *
   * @param tags list of HBase tags.
   * @return AggregationOperation
   */
  public static AggregationOperation getAggregationOperationFromTagsList(
      List<Tag> tags) {
    for (AggregationOperation aggOp : AggregationOperation.values()) {
      for (Tag tag : tags) {
        if (tag.getType() == aggOp.getTagType()) {
          return aggOp;
        }
      }
    }
    return null;
  }

  // flush and compact all the regions of the primary table

  /**
   * Flush and compact all regions of a table.
   * @param server region server
   * @param table the table to flush and compact
   * @throws IOException any IOE raised, or translated exception.
   * @return the number of regions flushed and compacted
   */
  public static int flushCompactTableRegions(HRegionServer server,
      TableName table) throws IOException {
    List<HRegion> regions = server.getRegions(table);
    for (HRegion region : regions) {
      region.flush(true);
      region.compact(true);
    }
    return regions.size();
  }

  /**
   * Check the existence of FlowRunCoprocessor in a table.
   * @param server region server
   * @param table  table to check
   * @param existenceExpected true if the FlowRunCoprocessor is expected
   *                         to be loaded in the table, false otherwise
   * @throws Exception  Exception if any.
   */
  public static void validateFlowRunCoprocessor(HRegionServer server,
      TableName table, boolean existenceExpected) throws Exception {
    List<HRegion> regions = server.getRegions(table);
    for (HRegion region : regions) {
      boolean found = false;
      Set<String> coprocs = region.getCoprocessorHost().getCoprocessors();
      for (String coprocName : coprocs) {
        if (coprocName.contains("FlowRunCoprocessor")) {
          found = true;
        }
      }
      if (found != existenceExpected) {
        throw new Exception("FlowRunCoprocessor is" +
            (existenceExpected ? " not " : " ") + "loaded in table " + table);
      }
    }
  }
}
