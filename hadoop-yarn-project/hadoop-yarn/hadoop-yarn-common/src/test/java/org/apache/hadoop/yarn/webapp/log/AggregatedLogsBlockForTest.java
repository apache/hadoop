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

package org.apache.hadoop.yarn.webapp.log;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.conf.Configuration;

public class AggregatedLogsBlockForTest extends AggregatedLogsBlock {

  final private Map<String, String> params = new HashMap<String, String>();
  private HttpServletRequest request;
  public AggregatedLogsBlockForTest(Configuration conf) {
    super(conf);
  }

  @Override
  public void render(Block html) {
    super.render(html);
  }

  public Map<String, String> moreParams() {
    return params;
  }

  public HttpServletRequest request() {
    return request;
  }
  public  void setRequest(HttpServletRequest request) {
     this.request = request;
  }

}
