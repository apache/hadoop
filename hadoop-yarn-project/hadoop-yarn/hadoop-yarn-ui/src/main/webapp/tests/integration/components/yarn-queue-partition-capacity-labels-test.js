import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('yarn-queue-partition-capacity-labels', 'Integration | Component | yarn queue partition capacity labels', {
  integration: true
});

test('it renders', function(assert) {
  
  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });" + EOL + EOL +

  this.render(hbs`{{yarn-queue-partition-capacity-labels}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#yarn-queue-partition-capacity-labels}}
      template block text
    {{/yarn-queue-partition-capacity-labels}}
  `);

  assert.equal(this.$().text().trim(), 'template block text');
});
