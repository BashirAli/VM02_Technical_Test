import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from beam_logic.beam_operations import VM02_Comp_Transform

# Our static input data, which will make up the initial PCollection.
transaction_data = [
    {'timestamp': '2009-01-01 01:01:01 UTC', 'origin': 'origin_1',
     'destination': 'dest_1', 'transaction_amount': 5.1},
    {'timestamp': '2024-01-01 01:01:01 UTC', 'origin': 'origin_2',
     'destination': 'dest_2', 'transaction_amount': 10.1},
    {'timestamp': '2011-01-01 01:01:01 UTC', 'origin': 'origin_3',
     'destination': 'dest_3', 'transaction_amount': 15.1},
    {'timestamp': '2020-01-01 01:01:01 UTC', 'origin': 'origin_4',
     'destination': 'dest_4', 'transaction_amount': 20.1},
    {'timestamp': '2021-01-01 01:01:01 UTC', 'origin': 'origin_5',
     'destination': 'dest_5', 'transaction_amount': 25.1},
    {'timestamp': '2001-01-01 01:01:01 UTC', 'origin': 'origin_6',
     'destination': 'dest_6', 'transaction_amount': 30.1}
]

class VM02_Comp_Transform_Test(unittest.TestCase):
    def test_comp_transform(self):
        # Create a test pipeline.
        with TestPipeline() as test_pipe:
            # Input PCollection.
            input = test_pipe | beam.Create(transaction_data)

            # Apply the Custom Composite Transform
            output = input | VM02_Comp_Transform()

            # Assert on the results.
            assert_that(
                output,
                equal_to([
                    ('2020-01-01 01:01:01 UTC', 20.1),
                    ('2021-01-01 01:01:01 UTC', 25.1)])
            )

if __name__ == '__main__':
    unittest.main()
