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
function open_hostip_list(x0, x1) {
  close_hostip_list();
  var ips = new Array();
  for (var i = 0; i < liveNodes.length; i++) {
    var dn = liveNodes[i];
    var index = (dn.usedSpace / dn.capacity) * 100.0;
    if (index == 0) {
      index = 1;
    }
    //More than 100% do not care,so not record in 95%-100% bar
    if (index > x0 && index <= x1) {
      ips.push(dn.infoAddr.split(":")[0]);
    }
  }
  var ipsText = '';
  for (var i = 0; i < ips.length; i++) {
    ipsText += ips[i] + '\n';
  }
  var histogram_div = document.getElementById('datanode-usage-histogram');
  histogram_div.setAttribute('style', 'position: relative');
  var ips_div = document.createElement("textarea");
  ips_div.setAttribute('id', 'datanode_ips');
  ips_div.setAttribute('rows', '8');
  ips_div.setAttribute('cols', '14');
  ips_div.setAttribute('style', 'position: absolute;top: 0px;right: -38px;');
  ips_div.setAttribute('readonly', 'readonly');
  histogram_div.appendChild(ips_div);

  var close_div = document.createElement("div");
  histogram_div.appendChild(close_div);
  close_div.setAttribute('id', 'close_ips');
  close_div.setAttribute('style', 'position: absolute;top: 0px;right: -62px;width:20px;height;20px');
  close_div.setAttribute('onclick', 'close_hostip_list()');
  close_div.innerHTML = "X";
  ips_div.innerHTML = ipsText;
}

function close_hostip_list() {
  $("#datanode_ips").remove();
  $("#close_ips").remove();
}
