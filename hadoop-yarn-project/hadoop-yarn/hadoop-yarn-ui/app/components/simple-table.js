import Ember from 'ember';

export default Ember.Component.extend({
  didInsertElement: function() {
    var paging = this.get("paging") ? true : this.get("paging");
    var ordering = this.get("ordering") ? true : this.get("ordering");
    var info = this.get("info") ? true : this.get("info");
    var bFilter = this.get("bFilter") ? true : this.get("bFilter");

    var colDefs = [];
    if (this.get("colTypes")) {
      var typesArr = this.get("colTypes").split(' ');
      var targetsArr = this.get("colTargets").split(' ');
      for (var i = 0; i < typesArr.length; i++) {
        colDefs.push({
          type: typesArr[i],
          targets: parseInt(targetsArr[i])
        });
      }
    }

    $('#' + this.get('table-id')).DataTable({
      "paging":   paging,
      "ordering": ordering, 
      "info":     info,
      "bFilter": bFilter,
      columnDefs: colDefs
    });
  }
});