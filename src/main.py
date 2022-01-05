import apache_beam as beam
from apache_beam.dataframe.convert import to_dataframe
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions

#print(INPUT_DATA_URL)

input_file = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'
output_path = '../output/results'


date_index = 0
origin_index = 1
destination_index = 2
transaction_index = 3
skip_header_lines = True


class VM02_Comp_Transform(beam.PTransform):
    def expand(self, p_collection):
        # for composite transforms, the expand method must be overriden with the main logic
        filtered_collection = (p_collection
                               | beam.Filter(self.transaction_filter)
                               | beam.Filter(self.date_filter)
                               | beam.Filter(self.date_agg)
                               )
        return filtered_collection

    def transaction_filter(self, element):
        return float(element[transaction_index]) > float(20)

    def date_filter(self, element):
        return datetime.strptime(element[date_index], "%Y-%m-%d %H:%M:%S %Z") > datetime.strptime('2010', "%Y")

    def date_agg(self, element):
        return element

class Standard_Beam_Operators():
    @staticmethod
    def read_as_csv(element):
        return element.split(",")


with beam.Pipeline() as pipe_line:
    collection = (pipe_line
                  | "Read File" >> beam.io.ReadFromText(input_file, skip_header_lines=skip_header_lines)
                  | "Read as CSV" >> beam.Map(Standard_Beam_Operators.read_as_csv)
                  | "Composite Transform" >> VM02_Comp_Transform()
                  | "Write to Output" >> beam.io.WriteToText(output_path, file_name_suffix='.csv')
                  | "Print" >> beam.Map(print)
                 )