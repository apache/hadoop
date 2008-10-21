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
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.Utilities;

@explain(displayName="Create Table")
public class createTableDesc extends ddlDesc implements Serializable 
{
  private static final long serialVersionUID = 1L;
  String              tableName;
  boolean             isExternal;
  List<FieldSchema>   cols;
  List<FieldSchema>   partCols;
  List<String>        bucketCols;
  List<Order>         sortCols;
  int                 numBuckets;
  String              fieldDelim;
  String              collItemDelim;
  String              mapKeyDelim;
  String              lineDelim;
  String              comment;
  boolean             isSequenceFile;
  String              location;
  String              serName;
  Map<String, String> mapProp;
  
  public createTableDesc(String tableName, boolean isExternal, 
                         List<FieldSchema> cols, List<FieldSchema> partCols,
                         List<String> bucketCols, List<Order> sortCols, 
                         int numBuckets, String fieldDelim, 
                         String collItemDelim,
                         String mapKeyDelim, String lineDelim, 
                         String comment, boolean isSequenceFile, 
                         String location, String serName, Map<String, String> mapProp) {
    this.tableName       = tableName;
    this.isExternal      = isExternal;
    this.bucketCols      = bucketCols;
    this.sortCols        = sortCols;
    this.collItemDelim   = collItemDelim;
    this.cols            = cols;
    this.comment         = comment;
    this.fieldDelim      = fieldDelim;
    this.isSequenceFile    = isSequenceFile;
    this.lineDelim       = lineDelim;
    this.location        = location;
    this.mapKeyDelim     = mapKeyDelim;
    this.numBuckets      = numBuckets;
    this.partCols        = partCols;
    this.serName         = serName;
    this.mapProp         = mapProp;
  }

  @explain(displayName="name")
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<FieldSchema> getCols() {
    return cols;
  }

  @explain(displayName="columns")
  public List<String> getColsString() {
    return Utilities.getFieldSchemaString(getCols());
  }
 
  public void setCols(List<FieldSchema> cols) {
    this.cols = cols;
  }

  public List<FieldSchema> getPartCols() {
    return partCols;
  }

  @explain(displayName="partition columns")
  public List<String> getPartColsString() {
    return Utilities.getFieldSchemaString(getPartCols());
  }

  public void setPartCols(List<FieldSchema> partCols) {
    this.partCols = partCols;
  }

  @explain(displayName="bucket columns")
  public List<String> getBucketCols() {
    return bucketCols;
  }

  public void setBucketCols(List<String> bucketCols) {
    this.bucketCols = bucketCols;
  }

  @explain(displayName="# buckets")
  public int getNumBuckets() {
    return numBuckets;
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  @explain(displayName="field delimiter")
  public String getFieldDelim() {
    return fieldDelim;
  }

  public void setFieldDelim(String fieldDelim) {
    this.fieldDelim = fieldDelim;
  }

  @explain(displayName="collection delimiter")
  public String getCollItemDelim() {
    return collItemDelim;
  }

  public void setCollItemDelim(String collItemDelim) {
    this.collItemDelim = collItemDelim;
  }

  @explain(displayName="map key delimiter")
  public String getMapKeyDelim() {
    return mapKeyDelim;
  }

  public void setMapKeyDelim(String mapKeyDelim) {
    this.mapKeyDelim = mapKeyDelim;
  }

  @explain(displayName="line delimiter")
  public String getLineDelim() {
    return lineDelim;
  }

  public void setLineDelim(String lineDelim) {
    this.lineDelim = lineDelim;
  }

  @explain(displayName="comment")
  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  @explain(displayName="isSequenceFile")
  public boolean isSequenceFile() {
    return isSequenceFile;
  }

  public void setSequenceFile(boolean isSequenceFile) {
    this.isSequenceFile = isSequenceFile;
  }

  @explain(displayName="location")
  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  @explain(displayName="isExternal")
  public boolean isExternal() {
    return isExternal;
  }

  public void setExternal(boolean isExternal) {
    this.isExternal = isExternal;
  }

  /**
   * @return the sortCols
   */
  @explain(displayName="sort columns")
  public List<Order> getSortCols() {
    return sortCols;
  }

  /**
   * @param sortCols the sortCols to set
   */
  public void setSortCols(List<Order> sortCols) {
    this.sortCols = sortCols;
  }

	/**
	 * @return the serDeName
	 */
  @explain(displayName="serde name")
	public String getSerName() {
		return serName;
	}

	/**
	 * @param serName the serName to set
	 */
	public void setSerName(String serName) {
		this.serName = serName;
	}

	/**
	 * @return the serDe properties
	 */
  @explain(displayName="serde properties")
  public Map<String,String> getMapProp() {
		return mapProp;
	}

	/**
	 * @param mapProp the map properties to set
	 */
	public void setMapProp(Map<String,String> mapProp) {
		this.mapProp = mapProp;
	}
  
}
