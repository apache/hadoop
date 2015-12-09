import Ember from 'ember';

export default Ember.Route.extend({
  model() {
  	var apps = this.store.findAll('yarnApp');
    return apps
  }
});
