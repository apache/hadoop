import Ember from 'ember';

export default Ember.Route.extend({
  model(param) {
    return Ember.RSVP.hash({
      selected : param.queue_name,
      queues: this.store.findAll('yarnQueue'),
      selectedQueue : undefined,
      apps: undefined, // apps of selected queue
    });
  },

  afterModel(model) {
    model.selectedQueue = this.store.peekRecord('yarnQueue', model.selected);
    model.apps = this.store.findAll('yarnApp');
    model.apps.forEach(function(o) {
      console.log(o);
    })
  }
});
