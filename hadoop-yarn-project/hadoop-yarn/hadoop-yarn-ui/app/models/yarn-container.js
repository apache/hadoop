import DS from 'ember-data';
import Converter from 'yarn-ui/utils/converter';

export default DS.Model.extend({
  allocatedMB: DS.attr('number'),
  allocatedVCores: DS.attr('number'),
  assignedNodeId: DS.attr('string'),
  priority: DS.attr('number'),
  startedTime: DS.attr('number'),
  finishedTime: DS.attr('number'),
  logUrl: DS.attr('string'),
  containerExitStatus: DS.attr('number'),
  containerState: DS.attr('string'),
  nodeHttpAddress: DS.attr('string'),

  startTs: function() {
    return Converter.dateToTimeStamp(this.get("startedTime"));
  }.property("startedTime"),

  finishedTs: function() {
    var ts = Converter.dateToTimeStamp(this.get("finishedTime"));
    return ts;
  }.property("finishedTime"),

  elapsedTime: function() {
    var elapsedMs = this.get("finishedTs") - this.get("startTs");
    if (elapsedMs <= 0) {
      elapsedMs = Date.now() - this.get("startTs");
    }

    return Converter.msToElapsedTime(elapsedMs);
  }.property(),

  tooltipLabel: function() {
    return "<p>Id:" + this.get("id") + 
           "</p><p>ElapsedTime:" + 
           String(this.get("elapsedTime")) + "</p>";
  }.property(),
});