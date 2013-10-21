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

  function render() {
    var helpers = {
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

    var base = dust.makeBase(helpers);

    var TEMPLATES = [ { 'name': 'dfshealth', 'url': 'dfshealth.dust.html' } ];

    load_templates(dust, TEMPLATES, function() {
      dust.render('dfshealth', base.push(data), function(err, out) {
        $('#panel').html(out);
      });
    }, function () {
      show_err_msg('Failed to load the page.');
    });
  }

  var BEANS = [
    {"name": "nn",      "url": "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo"},
    {"name": "nnstat",  "url": "/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"},
    {"name": "fs",      "url": "/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState"},
    {"name": "mem",     "url": "/jmx?qry=java.lang:type=Memory"},
    {"name": "startup", "url": "/startupProgress"}
  ];

  // Workarounds for the fact that JMXJsonServlet returns non-standard JSON strings
  function data_workaround(d) {
    function node_map_to_array(nodes) {
      var res = [];
      for (var n in nodes) {
        var p = nodes[n];
        p.name = n;
        res.push(p);
      }
      return res;
    }

    function startup_progress_workaround(r) {
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

    d.nn.JournalTransactionInfo = JSON.parse(d.nn.JournalTransactionInfo);
    d.nn.NameJournalStatus = JSON.parse(d.nn.NameJournalStatus);
    d.nn.NameDirStatuses = JSON.parse(d.nn.NameDirStatuses);
    d.nn.NodeUsage = JSON.parse(d.nn.NodeUsage);
    d.nn.LiveNodes = node_map_to_array(JSON.parse(d.nn.LiveNodes));
    d.nn.DeadNodes = node_map_to_array(JSON.parse(d.nn.DeadNodes));
    d.nn.DecomNodes = node_map_to_array(JSON.parse(d.nn.DecomNodes));
    d.nn.CorruptFiles = JSON.parse(d.nn.CorruptFiles);

    d.fs.SnapshotStats = JSON.parse(d.fs.SnapshotStats);
    d.startup = startup_progress_workaround(d.startup);
    return d;
  }

  function show_err_msg(msg) {
    $('#alert-panel-body').html(msg);
    $('#alert-panel').show();
  }

  load_json(
    BEANS,
    function(d) {
      for (var k in d) {
        data[k] = k === "startup" ? d[k] : d[k].beans[0];
      }
      data = data_workaround(data);
      render();
    },
    function (url, jqxhr, text, err) {
      show_err_msg('<p>Failed to retrieve data from ' + url + ', cause: ' + err + '</p>');
    });
})();
