from beam_logic.beam_operations import PipelineRunner


def main():
    """
    Main method which creates and runs an Apache Beam pipeline, then saves its output to a json1.gz file.
    Calls @PipelineRunner to do so
    :return:
    """
    pipeline_runner = PipelineRunner()
    pipeline_runner.run_pipeline()


if __name__ == '__main__':
    main()
