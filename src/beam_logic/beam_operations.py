import apache_beam as beam
from datetime import datetime
from beam_logic import constants


class VM02_Comp_Transform(beam.PTransform):
    # PTransform is the base class that afor composite transformations (basically grouped transformations)
    def expand(self, p_collection):
        # for composite transforms, the expand method must be overriden with the main logic
        filtered_collection = (p_collection
                               | "filters transactions" >> beam.Filter(self.transaction_filter)
                               | "filters dates" >> beam.Filter(self.date_filter)
                               | "Get tuples, mapping all transaction_amount to the same dates" >> beam.Map(
                    self.dict_to_tuple)
                               | "Gets total of remaining transactions by remaining timestamp" >> beam.CombinePerKey(
                    sum)
                               )
        return filtered_collection

    def transaction_filter(self, element):
        return float(element[constants.INPUT_METADATA['transaction_col']]) > float(constants.MAX_TRNSCTN_AMNT)

    def date_filter(self, element):
        return datetime.strptime(element["timestamp"], constants.INPUT_METADATA['timestamp_format']) \
               > datetime.strptime(constants.MIN_YEAR, constants.INPUT_METADATA['timestamp_format'])

    def dict_to_tuple(self, element):
        return (element[constants.INPUT_METADATA['timestamp_col']], element[constants.INPUT_METADATA['transaction_col']])


class Convert_CSV_To_Dict(beam.DoFn):
    # DoFn is the base class which hosts all logic that a ParDo needs to do
    # needs to override the process class
    def process(self, element):
        timestamp, origin, destination, transaction_amount = element.split(",")

        return [{
            constants.INPUT_METADATA['timestamp_col']: timestamp,
            constants.INPUT_METADATA['origin_col']: origin,
            constants.INPUT_METADATA['destination_col']: destination,
            constants.INPUT_METADATA['transaction_col']: float(transaction_amount)
        }]
