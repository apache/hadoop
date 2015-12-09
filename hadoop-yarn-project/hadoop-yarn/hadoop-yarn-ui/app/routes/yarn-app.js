import Ember from 'ember';

export default Ember.Route.extend({
  model(param) {
    return Ember.RSVP.hash({
      app: this.store.find('yarnApp', param.app_id),
      attempts: this.store.query('yarnAppAttempt', { appId: param.app_id})
    });
  }
});
