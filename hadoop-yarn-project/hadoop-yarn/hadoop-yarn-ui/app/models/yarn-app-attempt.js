import DS from 'ember-data';
import Converter from 'yarn-ui/utils/converter';

export default DS.Model.extend({
  startTime: DS.attr('string'),
  finishedTime: DS.attr('string'),
  containerId: DS.attr('string'),
  nodeHttpAddress: DS.attr('string'),
  nodeId: DS.attr('string'),
  logsLink: DS.attr('string'),

  startTs: function() {
    return Converter.dateToTimeStamp(this.get("startTime"));
  }.property("startTime"),

  finishedTs: function() {
    var ts = Converter.dateToTimeStamp(this.get("finishedTime"));
    return ts;
  }.property("finishedTime"),

  shortAppAttemptId: function() {
    return "attempt_" + 
           parseInt(Converter.containerIdToAttemptId(this.get("containerId")).split("_")[3]);
  }.property("containerId"),

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

  link: function() {
    return "/yarnAppAttempt/" + this.get("id");
  }.property(),
});