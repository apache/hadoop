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
(function () {
  "use strict";

  var data = {};
  var outstanding_requests = 2;

  dust.loadSource(dust.compile($('#tmpl-snn').html(), 'snn'));

  function show_error_msg(msg) {
    $('#alert-panel-body').html(msg);
    $('#alert-panel').show();
  }

  function finished_request() {
    outstanding_requests--;
    if (outstanding_requests == 0) {
      if (data.snn !== undefined && data.conf !== undefined) {
        var conf = data.conf;
        data.snn.CheckpointPeriod = conf['dfs.namenode.checkpoint.period'];
        data.snn.TxnCount = conf['dfs.namenode.checkpoint.txns'];
        render();
      } else {
        show_error_msg('Failed to load the information.');
      }
    }
  }

  function load() {
    $.getJSON('/jmx?qry=Hadoop:service=SecondaryNameNode,name=SecondaryNameNodeInfo', function(resp) {
      data.snn = resp.beans[0];
    }).always(finished_request);

    $.ajax({'url': '/conf', 'dataType': 'xml'}).done(function(d) {
      var $xml = $(d);
      var confs = {};
      $xml.find('property').each(function(idx,v) {
        confs[$(v).find('name').text()] = $(v).find('value').text();
      });
      data.conf = confs;
    }).always(finished_request);
  }

  function render() {
    dust.render('snn', data, function(err, out) {
      $('#tab-overview').html(out);
      $('#tab-overview').addClass('active');
    });
  }

  load();
})();
