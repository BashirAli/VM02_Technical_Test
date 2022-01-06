import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_logic.beam_operations import Convert_CSV_To_Dict, VM02_Comp_Transform

input_file = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'
output_path = '../output/results'

skip_header_lines = True

"""

beam_options = PipelineOptions()

class MyOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--input-file',
        default='gs://dataflow-samples/shakespeare/kinglear.txt',
        help='The file path for the input text to process.')
    parser.add_argument(
        '--output-path',
        required=True,
        help='The path prefix for output files.')
"""


def main():
    with beam.Pipeline() as pipe_line:
        collection = (pipe_line
                      | "Read File" >> beam.io.ReadFromText(input_file, skip_header_lines=skip_header_lines)
                      | "Read lines in to dict" >> beam.ParDo(Convert_CSV_To_Dict())
                      | "Composite Transform" >> VM02_Comp_Transform()
                      | "Write to Output" >> beam.io.WriteToText(output_path, file_name_suffix='.jsonl.gz')
                      | "Print" >> beam.Map(print)
                      )
        # output is a PCollection which is basically a dataset


if __name__ == '__main__':
    main()
