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

package org.apache.hadoop.metrics2.sink.ganglia;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This code supports Ganglia 3.1
 *
 */
public class GangliaSink31 extends GangliaSink30 {

  public final Log LOG = LogFactory.getLog(this.getClass());

  /**
   * The method sends metrics to Ganglia servers. The method has been taken from
   * org.apache.hadoop.metrics.ganglia.GangliaContext31 with minimal changes in
   * order to keep it in sync.

  * @param groupName The group name of the metric
   * @param name The metric name
   * @param type The type of the metric
   * @param value The value of the metric
   * @param gConf The GangliaConf for this metric
   * @param gSlope The slope for this metric
   * @throws IOException
   */
  protected void emitMetric(String groupName, String name, String type,
      String value, GangliaConf gConf, GangliaSlope gSlope)
    throws IOException {

    if (name == null) {
      LOG.warn("Metric was emitted with no name.");
      return;
    } else if (value == null) {
      LOG.warn("Metric name " + name +" was emitted with a null value.");
      return;
    } else if (type == null) {
      LOG.warn("Metric name " + name + ", value " + value + " has no type.");
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Emitting metric " + name + ", type " + type + ", value " + value
          + ", slope " + gSlope.name()+ " from hostname " + getHostName());
    }

    // The following XDR recipe was done through a careful reading of
    // gm_protocol.x in Ganglia 3.1 and carefully examining the output of
    // the gmetric utility with strace.

    // First we send out a metadata message
    xdr_int(128);               // metric_id = metadata_msg
    xdr_string(getHostName());       // hostname
    xdr_string(name);           // metric name
    xdr_int(0);                 // spoof = False
    xdr_string(type);           // metric type
    xdr_string(name);           // metric name
    xdr_string(gConf.getUnits());    // units
    xdr_int(gSlope.ordinal());  // slope
    xdr_int(gConf.getTmax());        // tmax, the maximum time between metrics
    xdr_int(gConf.getDmax());        // dmax, the maximum data value
    xdr_int(1);                 /*Num of the entries in extra_value field for
                                  Ganglia 3.1.x*/
    xdr_string("GROUP");        /*Group attribute*/
    xdr_string(groupName);      /*Group value*/

    // send the metric to Ganglia hosts
    emitToGangliaHosts();

    // Now we send out a message with the actual value.
    // Technically, we only need to send out the metadata message once for
    // each metric, but I don't want to have to record which metrics we did and
    // did not send.
    xdr_int(133);         // we are sending a string value
    xdr_string(getHostName()); // hostName
    xdr_string(name);     // metric name
    xdr_int(0);           // spoof = False
    xdr_string("%s");     // format field
    xdr_string(value);    // metric value

    // send the metric to Ganglia hosts
    emitToGangliaHosts();
  }
}
