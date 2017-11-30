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

  dust.loadSource(dust.compile($('#tmpl-federationhealth').html(), 'federationhealth'));
  dust.loadSource(dust.compile($('#tmpl-namenode').html(), 'namenode-info'));
  dust.loadSource(dust.compile($('#tmpl-datanode').html(), 'datanode-info'));
  dust.loadSource(dust.compile($('#tmpl-mounttable').html(), 'mounttable'));

  $.fn.dataTable.ext.order['ng-value'] = function (settings, col)
  {
    return this.api().column(col, {order:'index'} ).nodes().map(function (td, i) {
      return $(td).attr('ng-value');
    });
  };

  function load_overview() {
    var BEANS = [
      {"name": "federation",      "url": "/jmx?qry=Hadoop:service=Router,name=FederationState"}
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
      },

      'helper_date_tostring' : function (chunk, ctx, bodies, params) {
        var value = dust.helpers.tap(params.value, chunk, ctx);
        return chunk.write('' + new Date(Number(value)).toLocaleString());
      }
    };

    var data = {};

    // Workarounds for the fact that JMXJsonServlet returns non-standard JSON strings
    function workaround(nn) {
      nn.NodeUsage = JSON.parse(nn.NodeUsage);
      return nn;
    }

    load_json(
      BEANS,
      guard_with_startup_progress(function(d) {
        for (var k in d) {
          data[k] = k === 'federation' ? workaround(d[k].beans[0]) : d[k].beans[0];
        }
        render();
      }),
      function (url, jqxhr, text, err) {
        show_err_msg('<p>Failed to retrieve data from ' + url + ', cause: ' + err + '</p>');
      });

    function render() {
      var base = dust.makeBase(HELPERS);
      dust.render('federationhealth', base.push(data), function(err, out) {
        $('#tab-overview').html(out);
        $('#ui-tabs a[href="#tab-overview"]').tab('show');
      });
    }
  }

  function load_namenode_info() {
    var HELPERS = {
      'helper_lastcontact_tostring' : function (chunk, ctx, bodies, params) {
        var value = dust.helpers.tap(params.value, chunk, ctx);
        return chunk.write('' + new Date(Date.now()-1000*Number(value)));
      }
    };

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

      function capitalise(string) {
          return string.charAt(0).toUpperCase() + string.slice(1).toLowerCase();
      }

      function augment_namenodes(nodes) {
        for (var i = 0, e = nodes.length; i < e; ++i) {
          var n = nodes[i];
          n.usedPercentage = Math.round(n.used * 1.0 / n.totalSpace * 100);
          n.title = "Unavailable";
          n.iconState = "down";
          if (n.isSafeMode === true) {
            n.title = capitalise(n.state) + " (safe mode)"
            n.iconState = "decommisioned";
          } else if (n.state === "ACTIVE") {
            n.title = capitalise(n.state);
            n.iconState = "alive";
          } else if (nodes[i].state === "STANDBY") {
            n.title = capitalise(n.state);
            n.iconState = "down-decommisioned";
          } else if (nodes[i].state === "UNAVAILABLE") {
            n.title = capitalise(n.state);
            n.iconState = "down";
          }
          if (n.namenodeId === "null") {
            n.namenodeId = "";
          }
        }
      }

      r.Nameservices = node_map_to_array(JSON.parse(r.Nameservices));
      augment_namenodes(r.Nameservices);
      r.Namenodes = node_map_to_array(JSON.parse(r.Namenodes));
      augment_namenodes(r.Namenodes);
      return r;
    }

    $.get(
      '/jmx?qry=Hadoop:service=Router,name=FederationState',
      guard_with_startup_progress(function (resp) {
        var data = workaround(resp.beans[0]);
        var base = dust.makeBase(HELPERS);
        dust.render('namenode-info', base.push(data), function(err, out) {
          $('#tab-namenode').html(out);
          $('#ui-tabs a[href="#tab-namenode"]').tab('show');
        });
      })).error(ajax_error_handler);
  }

  // TODO Copied directly from dfshealth.js; is there a way to import this function?
  function load_datanode_info() {

    var HELPERS = {
      'helper_relative_time' : function (chunk, ctx, bodies, params) {
        var value = dust.helpers.tap(params.value, chunk, ctx);
        return chunk.write(moment().subtract(Number(value), 'seconds').format('YYYY-MM-DD HH:mm:ss'));
      },
      'helper_usage_bar' : function (chunk, ctx, bodies, params) {
        var value = dust.helpers.tap(params.value, chunk, ctx);
        var v = Number(value);
        var r = null;
        if (v < 70) {
          r = 'progress-bar-success';
        } else if (v < 85) {
          r = 'progress-bar-warning';
        } else {
          r = "progress-bar-danger";
        }
        return chunk.write(r);
      },
    };

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

      function augment_live_nodes(nodes) {
        for (var i = 0, e = nodes.length; i < e; ++i) {
          var n = nodes[i];
          n.usedPercentage = Math.round((n.used + n.nonDfsUsedSpace) * 1.0 / n.capacity * 100);
          if (n.adminState === "In Service") {
            n.state = "alive";
          } else if (nodes[i].adminState === "Decommission In Progress") {
            n.state = "decommisioning";
          } else if (nodes[i].adminState === "Decommissioned") {
            n.state = "decommissioned";
          }
        }
      }

      function augment_dead_nodes(nodes) {
        for (var i = 0, e = nodes.length; i < e; ++i) {
          if (nodes[i].decommissioned) {
            nodes[i].state = "down-decommissioned";
          } else {
            nodes[i].state = "down";
          }
        }
      }

      r.LiveNodes = node_map_to_array(JSON.parse(r.LiveNodes));
      augment_live_nodes(r.LiveNodes);
      r.DeadNodes = node_map_to_array(JSON.parse(r.DeadNodes));
      augment_dead_nodes(r.DeadNodes);
      r.DecomNodes = node_map_to_array(JSON.parse(r.DecomNodes));
      return r;
    }

    $.get(
      '/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo',
      guard_with_startup_progress(function (resp) {
        var data = workaround(resp.beans[0]);
        var base = dust.makeBase(HELPERS);
        dust.render('datanode-info', base.push(data), function(err, out) {
          $('#tab-datanode').html(out);
          $('#table-datanodes').dataTable( {
            'lengthMenu': [ [25, 50, 100, -1], [25, 50, 100, "All"] ],
            'columns': [
              { 'orderDataType': 'ng-value', 'searchable': true },
              { 'orderDataType': 'ng-value', 'type': 'numeric' },
              { 'orderDataType': 'ng-value', 'type': 'numeric' },
              { 'orderDataType': 'ng-value', 'type': 'numeric'}
            ]});
          $('#ui-tabs a[href="#tab-datanode"]').tab('show');
        });
      })).error(ajax_error_handler);
  }

  function load_mount_table() {
    var HELPERS = {}

    function workaround(resource) {
      resource.MountTable = JSON.parse(resource.MountTable)
      return resource;
    }

    $.get(
      '/jmx?qry=Hadoop:service=Router,name=FederationState',
      guard_with_startup_progress(function (resp) {
        var data = workaround(resp.beans[0]);
        var base = dust.makeBase(HELPERS);
        dust.render('mounttable', base.push(data), function(err, out) {
          $('#tab-mounttable').html(out);
          $('#ui-tabs a[href="#tab-mounttable"]').tab('show');
        });
      })).error(ajax_error_handler);
  }

  function toTitleCase(str) {
    return str.replace(/\w\S*/g, function(txt){
        return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
      });
  }

  function show_err_msg(msg) {
    $('#alert-panel-body').html(msg);
    $('#alert-panel').show();
  }

  function ajax_error_handler(url, jqxhr, text, err) {
    show_err_msg('<p>Failed to retrieve data from ' + url + ', cause: ' + err + '</p>');
  }

  function guard_with_startup_progress(fn) {
    return function() {
      try {
        fn.apply(this, arguments);
      } catch (err) {
        if (err instanceof TypeError) {
          show_err_msg('Router error: ' + err);
        }
      }
    };
  }

  function load_page() {
    var hash = window.location.hash;
    switch(hash) {
      case "#tab-mounttable":
        load_mount_table();
        break;
      case "#tab-namenode":
        load_namenode_info();
        break;
      case "#tab-datanode":
        load_datanode_info();
        break;
      case "#tab-overview":
        load_overview();
        break;
      default:
        window.location.hash = "tab-overview";
        break;
    }
  }
  load_page();

  $(window).bind('hashchange', function () {
    load_page();
  });
})();