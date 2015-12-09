import DS from 'ember-data';

export default DS.Model.extend({
  startedOn: DS.attr('string'),
  state: DS.attr('string'),
  haState: DS.attr('string'),
  rmStateStoreName: DS.attr('string'),
  resourceManagerVersion: DS.attr('string'),
  resourceManagerBuildVersion: DS.attr('string'),
  hadoopVersion: DS.attr('string'),
  hadoopBuildVersion: DS.attr('string'),
  hadoopVersionBuiltOn: DS.attr('string')
});