import DS from 'ember-data';

export default DS.Model.extend({
  name: DS.attr('string'),
  children: DS.attr('array'),
  parent: DS.attr('string'),
  capacity: DS.attr('number'),
  maxCapacity: DS.attr('number'),
  usedCapacity: DS.attr('number'),
  absCapacity: DS.attr('number'),
  absMaxCapacity: DS.attr('number'),
  absUsedCapacity: DS.attr('number'),
  state: DS.attr('string'),
  userLimit: DS.attr('number'),
  userLimitFactor: DS.attr('number'),
  preemptionDisabled: DS.attr('number'),
  numPendingApplications: DS.attr('number'),
  numActiveApplications: DS.attr('number'),
  users: DS.hasMany('YarnUser'),

  isLeafQueue: function() {
    var len = this.get("children.length");
    if (!len) {
      return true;
    }
    return len <= 0;
  }.property("children"),

  capacitiesBarChartData: function() {
    return [
      {
        label: "Absolute Capacity",
        value: this.get("name") == "root" ? 100 : this.get("absCapacity")
      },
      {
        label: "Absolute Used",
        value: this.get("name") == "root" ? this.get("usedCapacity") : this.get("absUsedCapacity")
      },
      {
        label: "Absolute Max Capacity",
        value: this.get("name") == "root" ? 100 : this.get("absMaxCapacity")
      }
    ]
  }.property("absCapacity", "absUsedCapacity", "absMaxCapacity"),

  userUsagesDonutChartData: function() {
    var data = [];
    if (this.get("users")) {
      this.get("users").forEach(function(o) {
        data.push({
          label: o.get("name"),
          value: o.get("usedMemoryMB")
        })
      });
    }

    return data;
  }.property("users"),

  hasUserUsages: function() {
    return this.get("userUsagesDonutChartData").length > 0;
  }.property(),

  numOfApplicationsDonutChartData: function() {
    return [
      {
        label: "Pending Apps",
        value: this.get("numPendingApplications") || 0 // TODO, fix the REST API so root will return #applications as well.
      },
      {
        label: "Active Apps",
        value: this.get("numActiveApplications") || 0
      }
    ]
  }.property(),
});
