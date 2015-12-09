import Ember from 'ember';

export default Ember.Route.extend({
  model() {
    return this.store.findAll('ClusterMetric');
  },

  afterModel() {
    this.controllerFor("ClusterOverview").set("loading", false);
  }
});