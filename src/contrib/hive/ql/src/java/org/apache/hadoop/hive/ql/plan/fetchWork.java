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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.tableDesc;

@explain(displayName="Fetch Operator")
public class fetchWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private Path tblDir;
  private tableDesc tblDesc;

  private List<Path> partDir;
  private List<partitionDesc> partDesc;

  private int limit;

  public fetchWork() { }

	public fetchWork(Path tblDir, tableDesc tblDesc, int limit) {
		this.tblDir = tblDir;
		this.tblDesc = tblDesc;
		this.limit = limit;
	}

	public fetchWork(List<Path> partDir, List<partitionDesc> partDesc, int limit) {
		this.partDir = partDir;
		this.partDesc = partDesc;
		this.limit = limit;
	}
	
	/**
	 * @return the tblDir
	 */
	public Path getTblDir() {
		return tblDir;
	}

	/**
	 * @param tblDir the tblDir to set
	 */
	public void setTblDir(Path tblDir) {
		this.tblDir = tblDir;
	}

	/**
	 * @return the tblDesc
	 */
	public tableDesc getTblDesc() {
		return tblDesc;
	}

	/**
	 * @param tblDesc the tblDesc to set
	 */
	public void setTblDesc(tableDesc tblDesc) {
		this.tblDesc = tblDesc;
	}

	/**
	 * @return the partDir
	 */
	public List<Path> getPartDir() {
		return partDir;
	}

	/**
	 * @param partDir the partDir to set
	 */
	public void setPartDir(List<Path> partDir) {
		this.partDir = partDir;
	}

	/**
	 * @return the partDesc
	 */
	public List<partitionDesc> getPartDesc() {
		return partDesc;
	}

	/**
	 * @param partDesc the partDesc to set
	 */
	public void setPartDesc(List<partitionDesc> partDesc) {
		this.partDesc = partDesc;
	}

	/**
	 * @return the limit
	 */
  @explain(displayName="limit")
	public int getLimit() {
		return limit;
	}

	/**
	 * @param limit the limit to set
	 */
	public void setLimit(int limit) {
		this.limit = limit;
	}
}
