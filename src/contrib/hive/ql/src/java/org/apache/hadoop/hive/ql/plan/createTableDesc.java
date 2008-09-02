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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

public class createTableDesc extends ddlDesc implements Serializable 
{
  private static final long serialVersionUID = 1L;
  String              tableName;
  boolean             isExternal;
  List<FieldSchema>   cols;
  List<FieldSchema>   partCols;
  List<String>        bucketCols;
  List<String>        sortCols;
  int                 numBuckets;
  String              fieldDelim;
  String              collItemDelim;
  String              mapKeyDelim;
  String              lineDelim;
  String              comment;
  boolean             isCompressed;
  String              location;
  
  public createTableDesc(String tableName, boolean isExternal, 
                         List<FieldSchema> cols, List<FieldSchema> partCols,
                         List<String> bucketCols, List<String> sortCols, 
                         int numBuckets, String fieldDelim, 
                         String collItemDelim,
                         String mapKeyDelim, String lineDelim, 
                         String comment, boolean isCompressed, 
                         String location) {
    this.tableName       = tableName;
    this.isExternal      = isExternal;
    this.bucketCols      = bucketCols;
    this.sortCols        = sortCols;
    this.collItemDelim   = collItemDelim;
    this.cols            = cols;
    this.comment         = comment;
    this.fieldDelim      = fieldDelim;
    this.isCompressed    = isCompressed;
    this.lineDelim       = lineDelim;
    this.location        = location;
    this.mapKeyDelim     = mapKeyDelim;
    this.numBuckets      = numBuckets;
    this.partCols        = partCols;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<FieldSchema> getCols() {
    return cols;
  }

  public void setCols(List<FieldSchema> cols) {
    this.cols = cols;
  }

  public List<FieldSchema> getPartCols() {
    return partCols;
  }

  public void setPartCols(List<FieldSchema> partCols) {
    this.partCols = partCols;
  }

  public List<String> getBucketCols() {
    return bucketCols;
  }

  public void setBucketCols(List<String> bucketCols) {
    this.bucketCols = bucketCols;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  public String getFieldDelim() {
    return fieldDelim;
  }

  public void setFieldDelim(String fieldDelim) {
    this.fieldDelim = fieldDelim;
  }

  public String getCollItemDelim() {
    return collItemDelim;
  }

  public void setCollItemDelim(String collItemDelim) {
    this.collItemDelim = collItemDelim;
  }

  public String getMapKeyDelim() {
    return mapKeyDelim;
  }

  public void setMapKeyDelim(String mapKeyDelim) {
    this.mapKeyDelim = mapKeyDelim;
  }

  public String getLineDelim() {
    return lineDelim;
  }

  public void setLineDelim(String lineDelim) {
    this.lineDelim = lineDelim;
  }

  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public boolean isCompressed() {
    return isCompressed;
  }

  public void setCompressed(boolean isCompressed) {
    this.isCompressed = isCompressed;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public boolean isExternal() {
    return isExternal;
  }

  public void setExternal(boolean isExternal) {
    this.isExternal = isExternal;
  }

  /**
   * @return the sortCols
   */
  public List<String> getSortCols() {
    return sortCols;
  }

  /**
   * @param sortCols the sortCols to set
   */
  public void setSortCols(List<String> sortCols) {
    this.sortCols = sortCols;
  }
}
