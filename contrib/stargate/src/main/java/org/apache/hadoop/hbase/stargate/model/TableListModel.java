/*
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.stargate.model;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.hbase.stargate.ProtobufMessageHandler;
import org.apache.hadoop.hbase.stargate.protobuf.generated.TableListMessage.TableList;

/**
 * Simple representation of a list of table names.
 */
@XmlRootElement(name="TableList")
public class TableListModel implements Serializable, ProtobufMessageHandler {

	private static final long serialVersionUID = 1L;

	private List<TableModel> tables = new ArrayList<TableModel>();

	/**
	 * Default constructor
	 */
	public TableListModel() {}

	/**
	 * Add the table name model to the list
	 * @param table the table model
	 */
	public void add(TableModel table) {
		tables.add(table);
	}
	
	/**
	 * @param index the index
	 * @return the table model
	 */
	public TableModel get(int index) {
		return tables.get(index);
	}

	/**
	 * @return the tables
	 */
	@XmlElementRef(name="table")
	public List<TableModel> getTables() {
		return tables;
	}

	/**
	 * @param tables the tables to set
	 */
	public void setTables(List<TableModel> tables) {
		this.tables = tables;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for(TableModel aTable : tables) {
			sb.append(aTable.toString());
			sb.append('\n');
		}
		return sb.toString();
	}

	@Override
	public byte[] createProtobufOutput() {
		TableList.Builder builder = TableList.newBuilder();
		for (TableModel aTable : tables) {
			builder.addName(aTable.getName());
		}
		return builder.build().toByteArray();
	}

  @Override
  public ProtobufMessageHandler getObjectFromMessage(byte[] message)
      throws IOException {
    TableList.Builder builder = TableList.newBuilder();
    builder.mergeFrom(message);
    for (String table: builder.getNameList()) {
      this.add(new TableModel(table));
    }
    return this;
  }
}
