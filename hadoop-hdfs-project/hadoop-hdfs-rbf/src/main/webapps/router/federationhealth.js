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
  dust.loadSource(dust.compile($('#tmpl-router').html(), 'router-info'));
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
      {"name": "federation",  "url": "jmx?qry=Hadoop:service=Router,name=FederationState"},
      {"name": "router",  "url": "jmx?qry=Hadoop:service=Router,name=Router"},
      {"name": "mem",         "url": "jmx?qry=java.lang:type=Memory"}
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
          if (k === 'router') {
            var routerInfo = d[k].beans[0];
            data[k].selfState = "unavailable";
            if (routerInfo.Safemode === true) {
              data[k].selfState = "safemode";
            } else if (routerInfo.RouterStatus === "INITIALIZING" || routerInfo.RouterStatus === "RUNNING") {
              data[k].selfState = "active";
            } else if (routerInfo.RouterStatus === "SAFEMODE") {
              data[k].selfState = "safemode";
            } else if (routerInfo.RouterStatus === "STOPPING") {
              data[k].selfState = "standby";
            } else if (routerInfo.RouterStatus === "UNAVAILABLE" || routerInfo.RouterStatus === "SHUTDOWN") {
              data[k].selfState = "unavailable";
            }
          }
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
          n.iconState = "unavailable";
          if (n.isSafeMode === true) {
            n.title = capitalise(n.state) + " (safe mode)"
            n.iconState = "safemode";
          } else if (n.state === "ACTIVE") {
            n.title = capitalise(n.state);
            n.iconState = "active";
          } else if (nodes[i].state === "OBSERVER") {
            n.title = capitalise(n.state);
            n.iconState = "observer";
          } else if (nodes[i].state === "STANDBY") {
            n.title = capitalise(n.state);
            n.iconState = "standby";
          } else if (nodes[i].state === "UNAVAILABLE") {
            n.title = capitalise(n.state);
            n.iconState = "unavailable";
          } else if (nodes[i].state === "DISABLED") {
            n.title = capitalise(n.state);
            n.iconState = "disabled";
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
      'jmx?qry=Hadoop:service=Router,name=FederationState',
      guard_with_startup_progress(function (resp) {
        var data = workaround(resp.beans[0]);
        var base = dust.makeBase(HELPERS);
        dust.render('namenode-info', base.push(data), function(err, out) {
          $('#tab-namenode').html(out);
          $('#ui-tabs a[href="#tab-namenode"]').tab('show');
        });
      })).fail(ajax_error_handler);
  }

  function load_router_info() {
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

      function augment_routers(nodes) {
        for (var i = 0, e = nodes.length; i < e; ++i) {
          var n = nodes[i];
          n.title = "Unavailable"
          n.iconState = "unavailable";
          if (n.status === "INITIALIZING") {
            n.title = capitalise(n.status);
            n.iconState = "active";
          } else if (n.status === "RUNNING") {
            n.title = capitalise(n.status);
            n.iconState = "active";
          } else if (n.status === "SAFEMODE") {
            n.title = capitalise(n.status);
            n.iconState = "safemode";
          } else if (n.status === "STOPPING") {
            n.title = capitalise(n.status);
            n.iconState = "unavailable";
          } else if (n.status === "SHUTDOWN") {
            n.title = capitalise(n.status);
            n.iconState = "unavailable";
          }
        }
      }

      r.Routers = node_map_to_array(JSON.parse(r.Routers));
      augment_routers(r.Routers);
      return r;
    }

    $.get(
      'jmx?qry=Hadoop:service=Router,name=FederationState',
      guard_with_startup_progress(function (resp) {
        var data = workaround(resp.beans[0]);
        var base = dust.makeBase(HELPERS);
        dust.render('router-info', base.push(data), function(err, out) {
          $('#tab-router').html(out);
          $('#ui-tabs a[href="#tab-router"]').tab('show');
        });
      })).fail(ajax_error_handler);
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
          var port = n.infoAddr.split(":")[1];
          var securePort = n.infoSecureAddr.split(":")[1];
          var dnHost = n.name.split(":")[0];
          n.dnWebAddress = "http://" + dnHost + ":" + port;
          if (securePort != 0) {
            n.dnWebAddress = "https://" + dnHost + ":" + securePort;
          }

          if (n.adminState === "In Service") {
            n.state = "alive";
          } else if (nodes[i].adminState === "Decommission In Progress") {
            n.state = "decommissioning";
          } else if (nodes[i].adminState === "Decommissioned") {
            n.state = "decommissioned";
          } else if (nodes[i].adminState === "Entering Maintenance") {
            n.state = "entering-maintenance";
          } else if (nodes[i].adminState === "In Maintenance") {
            n.state = "in-maintenance";
          }
        }
      }

      function augment_dead_nodes(nodes) {
        for (var i = 0, e = nodes.length; i < e; ++i) {
          if (nodes[i].adminState === "Decommissioned") {
            nodes[i].state = "down-decommissioned";
          } else if (nodes[i].adminState === "In Maintenance") {
            nodes[i].state = "down-maintenance";
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
      r.EnteringMaintenanceNodes = node_map_to_array(JSON.parse(r.EnteringMaintenanceNodes));
      return r;
    }

    function renderHistogram(dnData) {
      var data = dnData.LiveNodes.map(function(dn) {
        return (dn.usedSpace / dn.capacity) * 100.0;
      });

      var formatCount = d3.format(",.0f");

      var widthCap = $("div.container").width();
      var heightCap = 150;

      var margin = {top: 10, right: 60, bottom: 30, left: 30},
          width = widthCap * 0.9,
          height = heightCap - margin.top - margin.bottom;

      var x = d3.scaleLinear()
          .domain([0.0, 100.0])
          .range([0, width]);

      var bins = d3.histogram()
          .domain(x.domain())
          .thresholds(x.ticks(20))
          (data);

      var y = d3.scaleLinear()
          .domain([0, d3.max(bins, function(d) { return d.length; })])
          .range([height, 0]);

      var svg = d3.select("#datanode-usage-histogram").append("svg")
          .attr("width", width + 50.0)
          .attr("height", height + margin.top + margin.bottom)
          .append("g")
          .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

      svg.append("text")
          .attr("x", (width / 2))
          .attr("y", heightCap - 6 - (margin.top / 2))
          .attr("text-anchor", "middle")
          .style("font-size", "15px")
          .text("Disk usage of each DataNode (%)");

      var bar = svg.selectAll(".bar")
          .data(bins)
          .enter().append("g")
          .attr("class", "bar")
          .attr("transform", function(d) { return "translate(" + x(d.x0) + "," + y(d.length) + ")"; });

      window.liveNodes = dnData.LiveNodes;

      bar.append("rect")
          .attr("x", 1)
          .attr("width", x(bins[0].x1) - x(bins[0].x0) - 1)
          .attr("height", function(d) { return height - y(d.length); })
          .attr("onclick", function (d) { return "open_hostip_list(" + d.x0 + "," + d.x1 + ")"; });

      bar.append("text")
          .attr("dy", ".75em")
          .attr("y", 6)
          .attr("x", (x(bins[0].x1) - x(bins[0].x0)) / 2)
          .attr("text-anchor", "middle")
          .text(function(d) { return formatCount(d.length); });

      svg.append("g")
          .attr("class", "axis axis--x")
          .attr("transform", "translate(0," + height + ")")
          .call(d3.axisBottom(x));
    }

    $.get(
      'jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo',
      guard_with_startup_progress(function (resp) {
        var data = workaround(resp.beans[0]);
        var base = dust.makeBase(HELPERS);
        dust.render('datanode-info', base.push(data), function(err, out) {
          $('#tab-datanode').html(out);
          $('#table-datanodes').dataTable( {
            'lengthMenu': [ [25, 50, 100, -1], [25, 50, 100, "All"] ],
            'columnDefs': [
              { 'targets': [ 0 ], 'visible': false, 'searchable': false }
            ],
            'columns': [
              { 'orderDataType': 'ng-value', 'searchable': true , "defaultContent": "" },
              { 'orderDataType': 'ng-value', 'searchable': true , "defaultContent": "" },
              { 'orderDataType': 'ng-value', 'searchable': true , "defaultContent": ""},
              { 'orderDataType': 'ng-value', 'type': 'num' , "defaultContent": 0},
              { 'orderDataType': 'ng-value', 'type': 'num' , "defaultContent": 0},
              { 'orderDataType': 'ng-value', 'type': 'num' , "defaultContent": 0},
              { 'orderDataType': 'ng-value', 'type': 'num' , "defaultContent": 0},
              { 'orderDataType': 'ng-value', 'type': 'num' , "defaultContent": 0},
              { 'type': 'num' , "defaultContent": 0},
              { 'orderDataType': 'ng-value', 'type': 'num' , "defaultContent": 0},
              { 'type': 'string' , "defaultContent": ""}
            ],
            initComplete: function () {
              var column = this.api().column([0]);
              var select = $('<select class="datanodestatus form-control input-sm"><option value="">All</option></select>')
                  .appendTo('#datanodefilter')
                  .on('change', function () {
                    var val = $.fn.dataTable.util.escapeRegex(
                        $(this).val());
                    column.search(val ? '^' + val + '$' : '', true, false).draw();
                  });
              console.log(select);
              column.data().unique().sort().each(function (d, j) {
                select.append('<option value="' + d + '">' + d + '</option>');
              });
            }
          });
          renderHistogram(data);
          $('#ui-tabs a[href="#tab-datanode"]').tab('show');
        });
      })).fail(ajax_error_handler);
  }

  function load_mount_table() {
    var HELPERS = {}

    function workaround(resource) {
      function augment_read_only(mountTable) {
        for (var i = 0, e = mountTable.length; i < e; ++i) {
          if (mountTable[i].readonly == true) {
            mountTable[i].readonly = "readonly"
            mountTable[i].status = "Read Only"
          } else {
            mountTable[i].readonly = "readwrite"
            mountTable[i].status = "Read Write"
          }
        }
      }

      function augment_fault_tolerant(mountTable) {
        for (var i = 0, e = mountTable.length; i < e; ++i) {
          if (mountTable[i].faulttolerant == true) {
            mountTable[i].faulttolerant = "true"
            mountTable[i].ftStatus = "Fault tolerant"
          } else {
            mountTable[i].faulttolerant = "false"
          }
        }
      }

      resource.MountTable = JSON.parse(resource.MountTable)
      augment_read_only(resource.MountTable)
      augment_fault_tolerant(resource.MountTable)
      return resource;
    }

    $.get(
      'jmx?qry=Hadoop:service=Router,name=FederationState',
      guard_with_startup_progress(function (resp) {
        var data = workaround(resp.beans[0]);
        var base = dust.makeBase(HELPERS);
        dust.render('mounttable', base.push(data), function(err, out) {
          $('#tab-mounttable').html(out);
          $('#ui-tabs a[href="#tab-mounttable"]').tab('show');
        });
      })).fail(ajax_error_handler);
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
      case "#tab-overview":
        load_overview();
        break;
      case "#tab-namenode":
        load_namenode_info();
        break;
      case "#tab-router":
        load_router_info();
        break;
      case "#tab-datanode":
        load_datanode_info();
        break;
      case "#tab-mounttable":
        load_mount_table();
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
