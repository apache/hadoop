import Ember from 'ember';

export default Ember.Component.extend({
  didInsertElement: function() {
    $(".js-example-basic-single").select2(
      {
        width: '100%',
        placeholder: "Select a queue"
      });
    var elementId = this.get("element-id");
    var prefix = this.get("prefix");

    var element = d3.select("#" + elementId);

    if (element) {
      this.get("model").forEach(function(o) {
        element.append("option").attr("value", o.get("name")).text(prefix + o.get("name"));
      });
    }
  }
});