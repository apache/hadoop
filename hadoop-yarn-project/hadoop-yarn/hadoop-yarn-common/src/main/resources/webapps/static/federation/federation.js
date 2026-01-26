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

$(document).ready(function() {
    var table = $('#rms').DataTable();
    $('#rms tbody').on('click', 'td.details-control', function () {
        var tr = $(this).closest('tr');
        var row = table.row(tr);
        if (row.child.isShown()) {
            row.child.hide();
            tr.removeClass('shown');
        } else {
            var capabilityArr = scTableData.filter(item => (item.subcluster === row.id()));
            var capabilityObj = JSON.parse(capabilityArr[0].capability).clusterMetrics;
            row.child(
                '<table style="line-height:25px;" >' +
                '   <tr>' +
                '      <td>' +
                '         <h3>Application Metrics</h3>' +
                '         ApplicationSubmitted* : ' + capabilityObj.appsSubmitted + ' </p>' +
                '         ApplicationCompleted* : ' + capabilityObj.appsCompleted + ' </p>' +
                '         ApplicationPending*   : ' + capabilityObj.appsPending + ' </p>' +
                '         ApplicationRunning*   : ' + capabilityObj.appsRunning + ' </p>' +
                '         ApplicationFailed*    : ' + capabilityObj.appsFailed + ' </p>' +
                '         ApplicationKilled*    : ' + capabilityObj.appsKilled + ' </p>' +
                '      </td>' +
                '      <td>' +
                '        <h3>Resource Metrics</h3>' +
                '        <h4>Memory</h4>' +
                '        Total Memory : ' + capabilityArr[0].totalmemory + ' </p>' +
                '        Reserved Memory : ' + capabilityArr[0].reservedmemory + ' </p>' +
                '        Available Memory : ' + capabilityArr[0].availablememory + ' </p>' +
                '        Allocated Memory : ' + capabilityArr[0].allocatedmemory + ' </p>' +
                '        Pending Memory : ' + capabilityArr[0].pendingmemory + ' </p>' +
                '        <hr />' +
                '        <h4>VirtualCores</h4>' +
                '        TotalVirtualCores : ' + capabilityObj.totalVirtualCores + ' </p>' +
                '        ReservedVirtualCores : ' + capabilityObj.reservedVirtualCores + ' </p>' +
                '        AvailableVirtualCore : ' + capabilityObj.availableVirtualCores + ' </p>' +
                '        AllocatedVirtualCores : '+ capabilityObj.allocatedVirtualCores + ' </p>' +
                '        PendingVirtualCores : ' + capabilityObj.pendingVirtualCores + ' </p>' +
                '        <h4>Containers</h4>' +
                '        ContainersAllocated : ' + capabilityObj.containersAllocated + ' </p>' +
                '        ContainersReserved : ' + capabilityObj.containersReserved + ' </p>' +
                '        ContainersPending : ' + capabilityObj.containersPending + ' </p>' +
                '     </td>' +
                '     <td>' +
                '        <h3>Node Metrics</h3>' +
                '         TotalNodes : ' + capabilityObj.totalNodes + ' </p>' +
                '         LostNodes : ' + capabilityObj.lostNodes + ' </p>' +
                '         UnhealthyNodes : ' + capabilityObj.unhealthyNodes + ' </p>' +
                '         DecommissioningNodes : ' + capabilityObj.decommissioningNodes + ' </p>' +
                '         DecommissionedNodes : ' + capabilityObj.decommissionedNodes + ' </p>' +
                '         RebootedNodes : ' + capabilityObj.rebootedNodes + ' </p>' +
                '         ActiveNodes : ' + capabilityObj.activeNodes + ' </p>' +
                '         ShutdownNodes : ' + capabilityObj.shutdownNodes + ' </p>' +
                '     </td>' +
                '  </tr>' +
                '</table>').show();
            tr.addClass('shown');
        }
    });
});
