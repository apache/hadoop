import Ember from 'ember';

export default Ember.Route.extend({
  model(param) {
    return Ember.RSVP.hash({
      attempt: this.store.findRecord('yarnAppAttempt', param.app_attempt_id),
      
      rmContainers: this.store.query('yarnContainer', 
        {
          app_attempt_id: param.app_attempt_id,
          is_rm: true
        }),
      
      tsContainers: this.store.query('yarnContainer', 
        {
          app_attempt_id: param.app_attempt_id,
          is_rm: false
        }),
    });
  }
});