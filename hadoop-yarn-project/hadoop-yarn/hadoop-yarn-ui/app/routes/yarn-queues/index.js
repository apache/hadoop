export default Ember.Route.extend({
  beforeModel() {
    this.transitionTo('yarnQueues.root');
  }
});