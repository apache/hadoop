/**
 * Copyright 2007 The Apache Software Foundation
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
package org.apache.hadoop.hbase.rest.parser;

import java.util.ArrayList;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.rest.descriptors.RowUpdateDescriptor;
import org.apache.hadoop.hbase.rest.descriptors.ScannerDescriptor;
import org.apache.hadoop.hbase.rest.exception.HBaseRestException;

/**
 * 
 */
public interface IHBaseRestParser {
  /**
   * Parses a HTableDescriptor given the input array.
   * 
   * @param input
   * @return
   * @throws HBaseRestException
   */
  public HTableDescriptor getTableDescriptor(byte[] input)
      throws HBaseRestException;

  public ArrayList<HColumnDescriptor> getColumnDescriptors(byte[] input)
      throws HBaseRestException;

  public ScannerDescriptor getScannerDescriptor(byte[] input)
      throws HBaseRestException;

  public RowUpdateDescriptor getRowUpdateDescriptor(byte[] input,
      byte[][] pathSegments) throws HBaseRestException;
}
