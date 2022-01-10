"""
separate file which hosts all apache beam logic
Allows for code portability
"""
import apache_beam as beam
from datetime import datetime
from src.beam_logic import constants
from apache_beam.options.pipeline_options import PipelineOptions


class PipelineRunner:
    """
    Base class for running an apache beam pipeline
    """

    def __init__(self):
        """
        Constructor
        creates custom options for pipeline using the @CustomOptions class
        """
        self.beam_options = CustomOptions()

    def run_pipeline(self):
        """
        Method which runs the apache beam pipeline
        :return:
        """
        with beam.Pipeline(options=self.beam_options) as pipe_line:
            # view as returns the state of the PipelineOptions subclass i.e. @CustomOptions
            args = self.beam_options.view_as(CustomOptions)
            try:
                # collection is the resultant PCollection of the following pipeline (which is basically a dataset)
                # | is basically the apply function like in pandas
                # beam ReadFromText is file reader
                # beam ParDo is similar to Map, but enables parallel processing
                # VM02_Comp_Transform() is custom composite transform class
                # beam WriteToText is file writer
                collection = (pipe_line
                              | "Read File" >> beam.io.ReadFromText(args.input_file,
                                                                    skip_header_lines=constants.skip_header_lines)
                              | "Read lines in to dict" >> beam.ParDo(Convert_CSV_To_Dict())
                              | "Composite Transform" >> VM02_Comp_Transform()
                              | "Write to Output" >> beam.io.WriteToText(args.output_path, file_name_suffix='.jsonl.gz')
                              # | "Print" >> beam.Map(print)
                              )
            except Exception as e:
                print("Error occurred", str(e))


class VM02_Comp_Transform(beam.PTransform):
    """
    PTransform is the base class that are used for composite transformation classes (basically grouped transformations)
    """

    def expand(self, p_collection):
        """
        for composite transforms, the expand method must be overridden with the main logic
        :param p_collection: the PCollection dataset to work on
        :return: A PCollection of transformed data
        """
        # beam Filter filters out elements in PCollection based on a boolean value
        # beam Map applies a transformation to each elements in PCollection (1 to 1)
        # beam CombinePerKey(sum) is a grouping method which sum all values to the same key (basically df.groupby().sum() in pandas)
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
        """
        Filters the 'transaction_amount' value in the PCollection element by a constant. i.e. if its bigger than the
        constant, then keep it, otherwise drop it.
        :param element: A single item within a PCollection
        :return: Boolean, determining whether the element in PCollection meets criteria
        """
        return float(element[constants.INPUT_METADATA['transaction_col']]) > float(constants.MIN_TRNSCTN_AMNT)

    def date_filter(self, element):
        """
        Filters the 'timestamp' value in the PCollection element by a constant. i.e. if its bigger than the
        constant, then keep it, otherwise drop it.
        :param element: A single item within a PCollection
        :return: Boolean, determining whether the element in PCollection meets criteria
        """
        return datetime.strptime(element["timestamp"], constants.INPUT_METADATA['timestamp_format']) \
               > datetime.strptime(constants.MIN_YEAR, constants.INPUT_METADATA['timestamp_format'])

    def dict_to_tuple(self, element):
        """
        Turns a dictionary in to a tuple
        :param element:An element in the PCollection, which is of type dict
        :return: Tuple of (timestamp, transaction_amount)
        """
        return (
            element[constants.INPUT_METADATA['timestamp_col']], element[constants.INPUT_METADATA['transaction_col']])


class Convert_CSV_To_Dict(beam.DoFn):
    """
    Convert_CSV_To_Dict is a sub class of DoFn which splits item in a PCollection in to a dictionary
    Note: DoFn is the base class which hosts all logic that a ParDo needs to do.
    """

    def process(self, element):
        """
        The main method in a DoFn class which needs to be overridden. This method splits item in PCollection by ','
        in order to assign it under the correct key in the returned dictionary
        :param element: item in PCollection to process
        :return: A list of dictionaries - this type is effectively similar to an item in a PCollection
        """
        timestamp, origin, destination, transaction_amount = element.split(",")

        return [{
            constants.INPUT_METADATA['timestamp_col']: timestamp,
            constants.INPUT_METADATA['origin_col']: origin,
            constants.INPUT_METADATA['destination_col']: destination,
            constants.INPUT_METADATA['transaction_col']: float(transaction_amount)
        }]


class CustomOptions(PipelineOptions):
    """
    CustomOptions class which inherits PipelineOptions to allow for user provided options when running the pipeline
    """

    @classmethod
    def _add_argparse_args(cls, parser):
        """
        method which adds custom parameters for user. Includes options for input and output paths with additional help
        directives
        :param parser: Argument Parser
        :return:
        """
        parser.add_argument(
            '--input-file',
            default='gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv',
            help=' File path to input file.')
        parser.add_argument(
            '--output-path',
            default='../output/results',
            help='File path to which output files are saved to.')
