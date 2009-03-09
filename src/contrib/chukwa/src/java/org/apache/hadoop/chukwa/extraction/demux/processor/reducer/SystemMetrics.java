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

package org.apache.hadoop.chukwa.extraction.demux.processor.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class SystemMetrics  implements ReduceProcessor {
	static Logger log = Logger.getLogger(SystemMetrics.class);
	@Override
	public String getDataType() {
		return this.getClass().getName();
	}

	@Override
	public void process(ChukwaRecordKey key, 
						Iterator<ChukwaRecord> values,
						OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
						Reporter reporter) {
		try {
			
			ChukwaRecord record = null;
			ChukwaRecord newRecord = new ChukwaRecord();
      
			while(values.hasNext()) {
				record = values.next();
				newRecord.setTime(record.getTime());

				if(record.containsField("IFACE")) {
					if(record.containsField("rxpck/s")) {
						if (record.containsField("rxbyt/s") && record.containsField("txbyt/s")) {
							double netBusyPcnt=0, netRxByts=0, netTxByts=0, netSpeed=128000000.00;
							netRxByts=Double.parseDouble(record.getValue("rxbyt/s"));
							netTxByts=Double.parseDouble(record.getValue("txbyt/s"));
							netBusyPcnt=(netRxByts/netSpeed*100)+(netTxByts/netSpeed*100);
							record.add(record.getValue("IFACE")+"_busy_pcnt", "" + netBusyPcnt);
							record.add("csource", record.getValue("csource"));
						}						
						record.add(record.getValue("IFACE")+".rxbyt/s", record.getValue("rxbyt/s"));
						record.add(record.getValue("IFACE")+".rxpck/s", record.getValue("rxpck/s"));
						record.add(record.getValue("IFACE")+".txbyt/s", record.getValue("txbyt/s"));
						record.add(record.getValue("IFACE")+".txpck/s", record.getValue("txpck/s"));
						record.removeValue("rxbyt/s");
						record.removeValue("rxpck/s");
						record.removeValue("txbyt/s");
						record.removeValue("txpck/s");
					}
					if(record.containsField("rxerr/s")) {
						record.add(record.getValue("IFACE")+".rxerr/s", record.getValue("rxerr/s"));
						record.add(record.getValue("IFACE")+".rxdrop/s", record.getValue("rxdrop/s"));
						record.add(record.getValue("IFACE")+".txerr/s", record.getValue("txerr/s"));
						record.add(record.getValue("IFACE")+".txdrop/s", record.getValue("txdrop/s"));
						record.removeValue("rxerr/s");						
						record.removeValue("rxdrop/s");
						record.removeValue("txerr/s");
						record.removeValue("txdrop/s");
					}
					record.removeValue("IFACE");
				}
				
				if(record.containsField("Device:")) {
					record.add(record.getValue("Device:")+".r/s", record.getValue("r/s"));
					record.add(record.getValue("Device:")+".w/s", record.getValue("w/s"));
					record.add(record.getValue("Device:")+".rkB/s", record.getValue("rkB/s"));
					record.add(record.getValue("Device:")+".wkB/s", record.getValue("wkB/s"));
					record.add(record.getValue("Device:")+".%util", record.getValue("%util"));
					record.removeValue("r/s");
					record.removeValue("w/s");
					record.removeValue("rkB/s");
					record.removeValue("wkB/s");
					record.removeValue("%util");
					record.removeValue("Device:");
				}
				
				if (record.containsField("swap_free")) {
					float swapUsedPcnt=0, swapUsed=0, swapTotal=0;
					swapUsed=Long.parseLong(record.getValue("swap_used"));
					swapTotal=Long.parseLong(record.getValue("swap_total"));
					swapUsedPcnt=swapUsed/swapTotal*100;
					record.add("swap_used_pcnt", "" + swapUsedPcnt);
					record.add("csource", record.getValue("csource"));
				}
				
				if (record.containsField("mem_used")) {
					double memUsedPcnt=0, memTotal=0, memUsed=0;
					memTotal=Double.parseDouble(record.getValue("mem_total"));
					memUsed=Double.parseDouble(record.getValue("mem_used"));
					memUsedPcnt=memUsed/memTotal*100;
					record.add("mem_used_pcnt", "" + memUsedPcnt);
					record.add("csource", record.getValue("csource"));
				}
				
				if (record.containsField("mem_buffers")) {
					double memBuffersPcnt=0, memTotal=0, memBuffers=0;
					memTotal=Double.parseDouble(record.getValue("mem_total"));
					memBuffers=Double.parseDouble(record.getValue("mem_buffers"));
					memBuffersPcnt=memBuffers/memTotal*100;
					record.add("mem_buffers_pcnt", "" + memBuffersPcnt);
					record.add("csource", record.getValue("csource"));
				}
						
				// Copy over all fields
				String[] fields = record.getFields();
				for(String f: fields){
				  newRecord.add(f, record.getValue(f));
				}
			}
			record.add("capp", "systemMetrics");
			output.collect(key, newRecord);   
		} catch (IOException e) {
			log.warn("Unable to collect output in SystemMetricsReduceProcessor [" + key + "]", e);
			e.printStackTrace();
		}

	}
}
