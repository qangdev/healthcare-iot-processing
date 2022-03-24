# healthcare-iot-processing

### Todo
[] Create `bsm_data` collection
[] Create `bsm_agg_data` collection
[] Modify `Database` . Add methods to interact with database
[] Modify `RawDataModel` to make it capture and push raw data from `BSM` to AWS database
[] Modify `AggregateModel` . Add methods to process raw data and push result to `bsm_agg_data` collection
[] Modify `Main` to work with `Database`, `RawDataModel`, and `AggregateDataModel`
[] Modify `AlertDataModel`. Define *rules*
[] Modify `AlertDataModel`. Make it watch for record that violate *rules* and send the alert
[] Modify `Main` add `AlertDataModel`
[] Test all
