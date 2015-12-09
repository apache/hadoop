import DS from 'ember-data';

export default DS.JSONAPISerializer.extend({
    normalizeSingleResponse(store, primaryModelClass, payload, id,
      requestType) {
      var fixedPayload = {
        id: id,
        type: primaryModelClass.modelName,
        attributes: payload
      };

      return this._super(store, primaryModelClass, fixedPayload, id,
        requestType);
    },

    normalizeArrayResponse(store, primaryModelClass, payload, id,
      requestType) {
      // return expected is { data: [ {}, {} ] }
      var normalizedArrayResponse = {};

      // payload has apps : { app: [ {},{},{} ]  }
      // need some error handling for ex apps or app may not be defined.
      normalizedArrayResponse.data = [
        this.normalizeSingleResponse(store, primaryModelClass,
          payload.clusterInfo, payload.clusterInfo.id, requestType)
      ];
      return normalizedArrayResponse;
    }
});