import DS from 'ember-data';

export default DS.Model.extend({
  appsSubmitted: DS.attr('number'),
  appsCompleted: DS.attr('number'),
  appsPending: DS.attr('number'),
  appsRunning: DS.attr('number'),
  appsFailed: DS.attr('number'),
  appsKilled: DS.attr('number'),
  reservedMB: DS.attr('number'),
  availableMB: DS.attr('number'),
  allocatedMB: DS.attr('number'),
  reservedVirtualCores: DS.attr('number'),
  availableVirtualCores: DS.attr('number'),
  allocatedVirtualCores: DS.attr('number'),
  containersAllocated: DS.attr('number'),
  containersReserved: DS.attr('number'),
  containersPending: DS.attr('number'),
  totalMB: DS.attr('number'),
  totalVirtualCores: DS.attr('number'),
  totalNodes: DS.attr('number'),
  lostNodes: DS.attr('number'),
  unhealthyNodes: DS.attr('number'),
  decommissionedNodes: DS.attr('number'),
  rebootedNodes: DS.attr('number'),
  activeNodes: DS.attr('number'),

  getFinishedAppsDataForDonutChart: function() {
    var arr = [];
    arr.push({
      label: "Completed",
      value: this.get("appsCompleted")
    });
    arr.push({
      label: "Killed",
      value: this.get("appsKilled")
    });
    arr.push({
      label: "Failed",
      value: this.get("appsFailed")
    });

    return arr;
  }.property("appsCompleted", "appsKilled", "appsFailed"),

  getRunningAppsDataForDonutChart: function() {
    var arr = [];

    arr.push({
      label: "Pending",
      value: this.get("appsPending")
    });
    arr.push({
      label: "Running",
      value: this.get("appsRunning")
    });

    return arr;
  }.property("appsPending", "appsRunning"),

  getNodesDataForDonutChart: function() {
    var arr = [];
    arr.push({
      label: "Active",
      value: this.get("activeNodes")
    });
    arr.push({
      label: "Unhealthy",
      value: this.get("unhealthyNodes")
    });
    arr.push({
      label: "Decomissioned",
      value: this.get("decommissionedNodes")
    });
    return arr;
  }.property("activeNodes", "unhealthyNodes", "decommissionedNodes"),

  getMemoryDataForDonutChart: function() {
    var type = "MB";
    var arr = [];
    arr.push({
      label: "Allocated",
      value: this.get("allocated" + type)
    });
    arr.push({
      label: "Reserved",
      value: this.get("reserved" + type)
    });
    arr.push({
      label: "Available",
      value: this.get("available" + type)
    });

    return arr;
  }.property("allocatedMB", "reservedMB", "availableMB"),

  getVCoreDataForDonutChart: function() {
    var type = "VirtualCores";
    var arr = [];
    arr.push({
      label: "Allocated",
      value: this.get("allocated" + type)
    });
    arr.push({
      label: "Reserved",
      value: this.get("reserved" + type)
    });
    arr.push({
      label: "Available",
      value: this.get("available" + type)
    });

    return arr;
  }.property("allocatedVirtualCores", "reservedVirtualCores", "availableVirtualCores"),
});