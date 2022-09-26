$(document).ready(function() {
    // var scTableData = [{"capability":"{\n  \"clusterMetrics\":{\"appsSubmitted\":\"0\",\"appsCompleted\":\"0\",\"appsPending\":\"0\",\"appsRunning\":\"0\",\"appsFailed\":\"0\",\"appsKilled\":\"0\",\"reservedMB\":\"0\",\"availableMB\":\"4096\",\"allocatedMB\":\"0\",\"pendingMB\":\"0\",\"reservedVirtualCores\":\"0\",\"availableVirtualCores\":\"4\",\"allocatedVirtualCores\":\"0\",\"pendingVirtualCores\":\"0\",\"containersAllocated\":\"0\",\"containersReserved\":\"0\",\"containersPending\":\"0\",\"totalMB\":\"4096\",\"totalVirtualCores\":\"4\",\"utilizedMBPercent\":\"0\",\"utilizedVirtualCoresPercent\":\"0\",\"rmSchedulerBusyPercent\":\"-1\",\"totalNodes\":\"1\",\"lostNodes\":\"0\",\"unhealthyNodes\":\"0\",\"decommissioningNodes\":\"0\",\"decommissionedNodes\":\"0\",\"rebootedNodes\":\"0\",\"activeNodes\":\"1\",\"shutdownNodes\":\"0\",\"containerAssignedPerSecond\":\"0\",\"totalUsedResourcesAcrossPartition\":{\"memory\":\"0\",\"vCores\":\"0\",\"resourceInformations\":{\"resourceInformation\":[{\"attributes\":null,\"maximumAllocation\":\"9223372036854775807\",\"minimumAllocation\":\"0\",\"name\":\"memory-mb\",\"resourceType\":\"COUNTABLE\",\"units\":\"Mi\",\"value\":\"0\"},{\"attributes\":null,\"maximumAllocation\":\"9223372036854775807\",\"minimumAllocation\":\"0\",\"name\":\"vcores\",\"resourceType\":\"COUNTABLE\",\"units\":\"\",\"value\":\"0\"}]}},\"totalClusterResourcesAcrossPartition\":{\"memory\":\"4096\",\"vCores\":\"4\",\"resourceInformations\":{\"resourceInformation\":[{\"attributes\":null,\"maximumAllocation\":\"9223372036854775807\",\"minimumAllocation\":\"0\",\"name\":\"memory-mb\",\"resourceType\":\"COUNTABLE\",\"units\":\"Mi\",\"value\":\"4096\"},{\"attributes\":null,\"maximumAllocation\":\"9223372036854775807\",\"minimumAllocation\":\"0\",\"name\":\"vcores\",\"resourceType\":\"COUNTABLE\",\"units\":\"\",\"value\":\"4\"}]}},\"totalReservedResourcesAcrossPartition\":{\"memory\":\"0\",\"vCores\":\"0\",\"resourceInformations\":{\"resourceInformation\":[{\"attributes\":null,\"maximumAllocation\":\"9223372036854775807\",\"minimumAllocation\":\"0\",\"name\":\"memory-mb\",\"resourceType\":\"COUNTABLE\",\"units\":\"Mi\",\"value\":\"0\"},{\"attributes\":null,\"maximumAllocation\":\"9223372036854775807\",\"minimumAllocation\":\"0\",\"name\":\"vcores\",\"resourceType\":\"COUNTABLE\",\"units\":\"\",\"value\":\"0\"}]}},\"totalAllocatedContainersAcrossPartition\":\"0\",\"crossPartitionMetricsAvailable\":\"true\",\"rmEventQueueSize\":\"0\",\"schedulerEventQueueSize\":\"0\"}\n}","subcluster":"SC-1"},{"capability":"{\n  \"clusterMetrics\":{\"appsSubmitted\":\"0\",\"appsCompleted\":\"0\",\"appsPending\":\"0\",\"appsRunning\":\"0\",\"appsFailed\":\"0\",\"appsKilled\":\"0\",\"reservedMB\":\"0\",\"availableMB\":\"4096\",\"allocatedMB\":\"0\",\"pendingMB\":\"0\",\"reservedVirtualCores\":\"0\",\"availableVirtualCores\":\"4\",\"allocatedVirtualCores\":\"0\",\"pendingVirtualCores\":\"0\",\"containersAllocated\":\"0\",\"containersReserved\":\"0\",\"containersPending\":\"0\",\"totalMB\":\"4096\",\"totalVirtualCores\":\"4\",\"utilizedMBPercent\":\"0\",\"utilizedVirtualCoresPercent\":\"0\",\"rmSchedulerBusyPercent\":\"-1\",\"totalNodes\":\"1\",\"lostNodes\":\"0\",\"unhealthyNodes\":\"0\",\"decommissioningNodes\":\"0\",\"decommissionedNodes\":\"0\",\"rebootedNodes\":\"0\",\"activeNodes\":\"1\",\"shutdownNodes\":\"0\",\"containerAssignedPerSecond\":\"0\",\"totalUsedResourcesAcrossPartition\":{\"memory\":\"0\",\"vCores\":\"0\",\"resourceInformations\":{\"resourceInformation\":[{\"attributes\":null,\"maximumAllocation\":\"9223372036854775807\",\"minimumAllocation\":\"0\",\"name\":\"memory-mb\",\"resourceType\":\"COUNTABLE\",\"units\":\"Mi\",\"value\":\"0\"},{\"attributes\":null,\"maximumAllocation\":\"9223372036854775807\",\"minimumAllocation\":\"0\",\"name\":\"vcores\",\"resourceType\":\"COUNTABLE\",\"units\":\"\",\"value\":\"0\"}]}},\"totalClusterResourcesAcrossPartition\":{\"memory\":\"4096\",\"vCores\":\"4\",\"resourceInformations\":{\"resourceInformation\":[{\"attributes\":null,\"maximumAllocation\":\"9223372036854775807\",\"minimumAllocation\":\"0\",\"name\":\"memory-mb\",\"resourceType\":\"COUNTABLE\",\"units\":\"Mi\",\"value\":\"4096\"},{\"attributes\":null,\"maximumAllocation\":\"9223372036854775807\",\"minimumAllocation\":\"0\",\"name\":\"vcores\",\"resourceType\":\"COUNTABLE\",\"units\":\"\",\"value\":\"4\"}]}},\"totalReservedResourcesAcrossPartition\":{\"memory\":\"0\",\"vCores\":\"0\",\"resourceInformations\":{\"resourceInformation\":[{\"attributes\":null,\"maximumAllocation\":\"9223372036854775807\",\"minimumAllocation\":\"0\",\"name\":\"memory-mb\",\"resourceType\":\"COUNTABLE\",\"units\":\"Mi\",\"value\":\"0\"},{\"attributes\":null,\"maximumAllocation\":\"9223372036854775807\",\"minimumAllocation\":\"0\",\"name\":\"vcores\",\"resourceType\":\"COUNTABLE\",\"units\":\"\",\"value\":\"0\"}]}},\"totalAllocatedContainersAcrossPartition\":\"0\",\"crossPartitionMetricsAvailable\":\"true\",\"rmEventQueueSize\":\"0\",\"schedulerEventQueueSize\":\"0\"}\n}","subcluster":"SC-2"}];
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
            row.child('<table>' +
                      '   <tr>' +
                      '      <td> ' +
                      '         <h3>Application Metrics</h3>  ' +
                      '         ApplicationSubmitted* : '+ capabilityObj.appsSubmitted +' </p>' +
                      '         ApplicationCompleted* : '+ capabilityObj.appsCompleted +' </p>' +
                      '         ApplicationPending*   : '+ capabilityObj.appsPending +' </p>' +
                      '         ApplicationRunning*   : '+ capabilityObj.appsRunning +' </p>'+
                      '         ApplicationFailed*    : '+ capabilityObj.appsFailed +' </p>'+
                      '         ApplicationKilled*    : '+ capabilityObj.appsKilled +' </p>'+
                      '      </td>' +
                      '      <td>' +
                      '        <h3>Resource Metrics</h3>'+
                      '        <h4>Memory</h4>'+
                      '        TotalMB : '+ capabilityObj.totalMB +' </p>' +
                      '        ReservedMB : '+ capabilityObj.reservedMB +' </p>'+
                      '        AvailableMB : '+ capabilityObj.availableMB +' </p> '+
                      '        AllocatedMB : '+ capabilityObj.allocatedMB +' </p> '+
                      '        PendingMB : '+ capabilityObj.pendingMB +' </p>' +
                      '        <h4>VirtualCores</h4>'+
                      '        TotalVirtualCores : '+capabilityObj.totalVirtualCores+' </p>'+
                      '        ReservedVirtualCores : '+capabilityObj.reservedVirtualCores+' </p>'+
                      '        AvailableVirtualCore : '+capabilityObj.availableVirtualCores+' </p>'+
                      '        AllocatedVirtualCores : '+capabilityObj.allocatedVirtualCores+' </p>'+
                      '        PendingVirtualCores : '+capabilityObj.pendingVirtualCores+' </p>'+
                      '        <h4>Containers</h4>'+
                      '        ContainersAllocated : '+capabilityObj.containersAllocated+' </p>'+
                      '        ContainersReserved : '+capabilityObj.containersReserved+' </p>' +
                      '        ContainersPending : '+capabilityObj.containersPending+' </p>'+
                      '     </td>'+
                      '     <td>'+
                      '        <h3>Node Metrics</h3>'+
                      '         TotalNodes : '+capabilityObj.totalNodes+' </p>'+
                      '         LostNodes : '+capabilityObj.lostNodes+' </p>'+
                      '         UnhealthyNodes : '+capabilityObj.unhealthyNodes+' </p>'+
                      '         DecommissioningNodes : '+capabilityObj.decommissioningNodes+' </p>'+
                      '         DecommissionedNodes : '+capabilityObj.decommissionedNodes+' </p>'+
                      '         RebootedNodes : '+capabilityObj.rebootedNodes+' </p>'+
                      '         ActiveNodes : '+capabilityObj.activeNodes+' </p>'+
                      '         ShutdownNodes : '+capabilityObj.shutdownNodes+' </p>'+
                      '     </td>'+
                      '  </tr>'+
                      '</table>').show();
                      tr.addClass('shown');
        }
    });
});
