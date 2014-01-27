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

  dust.loadSource(dust.compile($('#tmpl-dfshealth').html(), 'dfshealth'));
  dust.loadSource(dust.compile($('#tmpl-startup-progress').html(), 'startup-progress'));
  dust.loadSource(dust.compile($('#tmpl-datanode').html(), 'datanode-info'));
  dust.loadSource(dust.compile($('#tmpl-snapshot').html(), 'snapshot-info'));

  function load_overview() {
    var BEANS = [
      {"name": "nn",      "url": "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"},
      {"name": "nnstat",  "url": "/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"},
      {"name": "fs",      "url": "/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState"},
      {"name": "mem",     "url": "/jmx?qry=java.lang:type=Memory"},
    ];

    var HELPERS = {
      'helper_fs_max_objects': function (chunk, ctx, bodies, params) {
        var o = ctx.current();
        if (o.MaxObjects > 0) {
          chunk.write('(' + Math.round((o.FilesTotal + o.BlockTotal) / o.MaxObjects * 100) * 100 + ')%');
        }
      },

      'helper_dir_status': function (chunk, ctx, bodies, params) {
        var j = ctx.current();
        for (var i in j) {
          chunk.write('<tr><td>' + i + '</td><td>' + j[i] + '</td><td>' + params.type + '</td></tr>');
        }
      }
    };

    var data = {};

    // Workarounds for the fact that JMXJsonServlet returns non-standard JSON strings
    function data_workaround(d) {
      d.nn.JournalTransactionInfo = JSON.parse(d.nn.JournalTransactionInfo);
      d.nn.NameJournalStatus = JSON.parse(d.nn.NameJournalStatus);
      d.nn.NameDirStatuses = JSON.parse(d.nn.NameDirStatuses);
      d.nn.NodeUsage = JSON.parse(d.nn.NodeUsage);
      d.nn.CorruptFiles = JSON.parse(d.nn.CorruptFiles);
      return d;
    }

    load_json(
      BEANS,
      function(d) {
        for (var k in d) {
          data[k] = d[k].beans[0];
        }
        data = data_workaround(data);
        render();
      },
      function (url, jqxhr, text, err) {
        show_err_msg('<p>Failed to retrieve data from ' + url + ', cause: ' + err + '</p>');
      });

    function render() {
      var base = dust.makeBase(HELPERS);
      dust.render('dfshealth', base.push(data), function(err, out) {
        $('#tab-overview').html(out);
        $('a[href="#tab-datanode"]').click(load_datanode_info);
        $('#ui-tabs a[href="#tab-overview"]').tab('show');
      });
    }
  }
  $('#ui-tabs a[href="#tab-overview"]').click(load_overview);

  function show_err_msg(msg) {
    $('#alert-panel-body').html(msg);
    $('#alert-panel').show();
  }

  function ajax_error_handler(url, jqxhr, text, err) {
    show_err_msg('<p>Failed to retrieve data from ' + url + ', cause: ' + err + '</p>');
  }

  function load_startup_progress() {
    function workaround(r) {
      function rename_property(o, s, d) {
        if (o[s] !== undefined) {
          o[d] = o[s];
          delete o[s];
        }
      }
      r.percentComplete *= 100;
      $.each(r.phases, function (idx, p) {
        p.percentComplete *= 100;
        $.each(p.steps, function (idx2, s) {
          s.percentComplete *= 100;
          // dust.js is confused by these optional keys in nested
          // structure, rename them
          rename_property(s, "desc", "stepDesc");
          rename_property(s, "file", "stepFile");
          rename_property(s, "size", "stepSize");
        });
      });
      return r;
    }
    $.get('/startupProgress', function (resp) {
      var data = workaround(resp);
      dust.render('startup-progress', data, function(err, out) {
        $('#tab-startup-progress').html(out);
        $('#ui-tabs a[href="#tab-startup-progress"]').tab('show');
      });
    }).error(ajax_error_handler);
  }

  $('#ui-tabs a[href="#tab-startup-progress"]').click(load_startup_progress);

  function load_datanode_info() {
    function workaround(r) {
      function node_map_to_array(nodes) {
        var res = [];
        for (var n in nodes) {
          var p = nodes[n];
          p.name = n;
          res.push(p);
        }
        return res;
      }

      r.LiveNodes = node_map_to_array(JSON.parse(r.LiveNodes));
      r.DeadNodes = node_map_to_array(JSON.parse(r.DeadNodes));
      r.DecomNodes = node_map_to_array(JSON.parse(r.DecomNodes));
      return r;
    }

    $.get('/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo', function (resp) {
      var data = workaround(resp.beans[0]);
      dust.render('datanode-info', data, function(err, out) {
        $('#tab-datanode').html(out);
        $('#ui-tabs a[href="#tab-datanode"]').tab('show');
      });
    }).error(ajax_error_handler);
  }

  $('a[href="#tab-datanode"]').click(load_datanode_info);

  function load_snapshot_info() {
    $.get('/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState', function (resp) {
      var data = JSON.parse(resp.beans[0].SnapshotStats);
      dust.render('snapshot-info', data, function(err, out) {
        $('#tab-snapshot').html(out);
        $('#ui-tabs a[href="#tab-snapshot"]').tab('show');
      });
    }).error(ajax_error_handler);
  }

  $('#ui-tabs a[href="#tab-snapshot"]').click(load_snapshot_info);

  var hash = window.location.hash;
  if (hash === "#tab-datanode") {
    load_datanode_info();
  } else if (hash === "#tab-snapshot") {
    load_snapshot_info();
  } else if (hash === "#tab-startup-progress") {
    load_startup_progress();
  } else {
    load_overview();
  }
})();
