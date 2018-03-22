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

  dust.loadSource(dust.compile($('#tmpl-dn').html(), 'dn'));

  function load() {
    $.get('/jmx?qry=Hadoop:service=DataNode,name=DataNodeInfo', function(resp) {
      data.dn = workaround(resp.beans[0]);
      data.dn.HostName = resp.beans[0]['DatanodeHostname'];
      render();
    }).fail(show_err_msg);
  }

  function workaround(dn) {
    function node_map_to_array(nodes) {
      var res = [];
      for (var n in nodes) {
        var p = nodes[n];
        p.name = n;
        res.push(p);
      }
      return res;
    }

    dn.VolumeInfo = node_map_to_array(JSON.parse(dn.VolumeInfo));
    dn.BPServiceActorInfo = JSON.parse(dn.BPServiceActorInfo);

    return dn;
  }

  function render() {
    var base = dust.makeBase({
      'helper_relative_time' : function (chunk, ctx, bodies, params) {
        var value = dust.helpers.tap(params.value, chunk, ctx);
        return chunk.write(moment().subtract(Number(value), 'seconds').fromNow(true));
      }
    });
    dust.render('dn', base.push(data), function(err, out) {
      $('#tab-overview').html(out);
      $('#tab-overview').addClass('active');
    });
  }

  function show_err_msg() {
    $('#alert-panel-body').html("Failed to load datanode information");
    $('#alert-panel').show();
  }

  load();

})();
