import DS from 'ember-data';

export default DS.Model.extend({
  name: DS.attr('string'),
  queueName: DS.attr('string'),
  usedMemoryMB: DS.attr('number'),
  usedVCore: DS.attr('number')
})