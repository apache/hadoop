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

package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

public class HadoopMetricsProcessor extends AbstractProcessor {

	static Logger log = Logger.getLogger(HadoopMetricsProcessor.class);
	static final String chukwaTimestampField = "chukwa_timestamp";
	static final String contextNameField = "contextName";
	static final String recordNameField = "recordName";

	private SimpleDateFormat sdf = null;

	public HadoopMetricsProcessor() {
		// TODO move that to config
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void parse(String recordEntry,
			OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
			Reporter reporter) throws Throwable {
		try {
			String dStr = recordEntry.substring(0, 23);
			int start = 24;
			int idx = recordEntry.indexOf(' ', start);
			// String level = recordEntry.substring(start, idx);
			start = idx + 1;
			idx = recordEntry.indexOf(' ', start);
			// String className = recordEntry.substring(start, idx-1);
			String body = recordEntry.substring(idx + 1);
			body.replaceAll("\n", "");
			// log.info("record [" + recordEntry + "] body [" + body +"]");
			Date d = sdf.parse(dStr);

			JSONObject json = new JSONObject(body);

			ChukwaRecord record = new ChukwaRecord();
			String datasource = null;
			String recordName = null;

			Iterator<String> ki = json.keys();
			while (ki.hasNext()) {
				String keyName = ki.next();
				if (chukwaTimestampField.intern() == keyName.intern()) {
					d = new Date(json.getLong(keyName));
					Calendar cal = Calendar.getInstance();
					cal.setTimeInMillis(d.getTime());
					cal.set(Calendar.SECOND, 0);
					cal.set(Calendar.MILLISECOND, 0);
					d.setTime(cal.getTimeInMillis());
				} else if (contextNameField.intern() == keyName.intern()) {
					datasource = "Hadoop_" + json.getString(keyName);
				} else if (recordNameField.intern() == keyName.intern()) {
					recordName = json.getString(keyName);
					record.add(keyName, json.getString(keyName));
				} else {
					record.add(keyName, json.getString(keyName));
				}
			}

			datasource = datasource + "_" + recordName;
			buildGenericRecord(record, null, d.getTime(), datasource);
			output.collect(key, record);
		} catch (ParseException e) {
			e.printStackTrace();
			log.warn("Wrong format in HadoopMetricsProcessor [" + recordEntry
					+ "]", e);
			throw e;
		} catch (IOException e) {
			e.printStackTrace();
			log.warn("Unable to collect output in HadoopMetricsProcessor ["
					+ recordEntry + "]", e);
			throw e;
		} catch (JSONException e) {
			e.printStackTrace();
			log.warn("Wrong format in HadoopMetricsProcessor [" + recordEntry
					+ "]", e);
			throw e;
		}

	}

	public String getDataType() {
		return HadoopMetricsProcessor.class.getName();
	}

}
