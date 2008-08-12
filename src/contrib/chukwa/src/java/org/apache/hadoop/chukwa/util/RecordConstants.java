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

package org.apache.hadoop.chukwa.util;

public class RecordConstants
{
  static final char[] CTRL_A =  {'\u0001'};
  static final char[] CTRL_B =  {'\u0002'};
  static final char[] CTRL_C =  {'\u0003'};
  static final char[] CTRL_D =  {'\u0004'};
	//public static final String FIELD_SEPARATOR = new String(CTRL_A);
	public static final String DEFAULT_FIELD_SEPARATOR = "-#-";
	public static final String DEFAULT_RECORD_SEPARATOR = "\n";
	public static final String RECORD_SEPARATOR_ESCAPE_SEQ = new String (CTRL_D);// may want this to be very obscure, e.g. new String(CTRL_B) + new String (CTRL_C) + new String (CTRL_D)
	
	/**
	 * Insert the default chukwa escape sequence in <code>record</code> before all occurances of 
	 * <code>recordSeparator</code> <i>except</i> the final one if the final record separator occurs
	 * at the end of the <code>record</code> 
	 * @param recordSeparator The record separator that we are escaping. This is chunk source application specific 
	 * @param record The string representing the entire record, including the final record delimiter
	 * @return The string with appropriate <code>recordSeparator</code>s escaped
	 */
	public static String escapeAllButLastRecordSeparator(String recordSeparator,String record){
	  String escapedRecord = "";
	  if (record.endsWith(recordSeparator)){
	    escapedRecord = record.substring(0,record.length()-recordSeparator.length()).replaceAll(recordSeparator, RECORD_SEPARATOR_ESCAPE_SEQ + recordSeparator) + recordSeparator;
	  }
	  return escapedRecord;
	}
	
  /**
   * Insert the default chukwa escape sequence in <code>record</code> before all occurances of 
   * <code>recordSeparator</code>. This is assuming that you are not passing the final record
   * separator in with the <code>record</code>, because it would be escaped too. 
   * @param recordSeparator The record separator that we are escaping. This is chunk source application specific 
   * @param record The string representing the entire record, including the final record delimiter
   * @return The string with all <code>recordSeparator</code>s escaped
   */
	 public static String escapeAllRecordSeparators(String recordSeparator,String record){
	      return record.replaceAll(recordSeparator, RECORD_SEPARATOR_ESCAPE_SEQ + recordSeparator);
	  }
	
	public static String recoverRecordSeparators(String recordSeparator, String record){
    return record.replaceAll(RECORD_SEPARATOR_ESCAPE_SEQ+recordSeparator, recordSeparator);
  }
	
}
