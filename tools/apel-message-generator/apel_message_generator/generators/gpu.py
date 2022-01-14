import os

from apel_message_generator.generators.record_generator import RecordGenerator

class GPURecordGenerator(RecordGenerator):
    """Generator for GPU messages."""

    def __init__(self, recs_per_msg, no_msgs, msg_path):
        """Define constants used by the GPU records."""

        super(GPURecordGenerator, self).__init__(recs_per_msg, no_msgs)

        if msg_path is None:
            self._msg_path = "gpu-msgs"
        else:
            self._msg_path = msg_dir
        self._msg_path = os.path.abspath(self._msg_path)

        self._msg_type = "APEL-GPU-message"
        self._msg_version = "0.1"

        # Fields which are required by the message format.
        self._mandatory_fields = [
            "SiteName", "GlobalUserName", "FQAN",
            "Count", "Cores", "AvailableDuration", "Type", "Model",
            "AssociatedRecordType", "ActiveDuration", "MeasurementYear",
            "MeasurementMonth"
        ]

        # This list allows us to specify the order of lines when we construct
        # records.
        self._all_fields = [
            "MeasurementMonth", "MeasurementYear", 
            "AssociatedRecordType", "AssociatedRecord", 
            "GlobalUserName", "FQAN", "SiteName", 
            "Count", "Cores", "ActiveDuration", "AvailableDuration", 
            "BenchmarkType", "Benchmark", 
            "Type", "Model",
            #"PublisherDNID"
        ]

        self._int_fields = ["MeasurementMonth", "MeasurementYear",
                            "ActiveDuration", "AvailableDuration",
                            "Cores"]

        self._float_fields = ["Count", "Benchmark"] 

        self._allowed_types = ['GPU', 'FPGA', 'Other']
        self._allowed_record_types = ['cloud',]


        RecordGenerator._get_optional_fields(self)


    def _get_record(self, keys, job_id):
        """Get a record, then add summary-specific items."""
        record = RecordGenerator._get_record(self, keys, job_id)
        record['GlobalUserName'] = get_random_string(dns)
        record['MeasurementMonth'] = get_random_int(end=12)
        record['MeasurementYear'] = get_random_int(2000, 2021)

        #record['Type'] = mix_enums(self._allowed_types)
        #record['associatedRecordType'] = mix_enums(self._allowed_record_types)

        record['Type'] = get_random_string(self._allowed_types)
        record['AssociatedRecordType'] = get_random_string(self._allowed_record_types)

        return record


class GPUSummaryGenerator(RecordGenerator):
    """Generator for GPU summaries."""

    def __init__(self, recs_per_msg, no_msgs, msg_path):
        """Define constants used by the GPU records."""

        super(GPUSummaryGenerator, self).__init__(recs_per_msg, no_msgs)

        if msg_path is None:
            self._msg_path = "gpusummary-msgs"
        else:
            self._msg_path = msg_dir
        self._msg_path = os.path.abspath(self._msg_path)

        self._msg_type = "APEL-GPU-summary-message"
        self._msg_version = "0.1"

        # Fields which are required by the message format.
        self._mandatory_fields = [
            'Month', 'Year', 'AssociatedRecordType', 'SiteName', 'Count', 
            'AvailableDuration', 'Type', 'NumberOfRecords',
        ]

        # This list allows us to specify the order of lines when we construct
        # records.
        self._all_fields = [
            'Month', 'Year', 'AssociatedRecordType', 'GlobalUserName', 'SiteName',
            'Count', 'Cores', 'AvailableDuration', 'ActiveDuration', 'BenchmarkType',
            'Benchmark', 'Type', 'Model', 'NumberOfRecords',
        ]

        self._int_fields = [
            'Month', 'Year', 'Cores', 'AvailableDuration', 'ActiveDuration',
            'NumberOfRecords'
        ]

        self._float_fields = [                
            'Count',
            'Benchmark'
        ] 

        self._allowed_types = ['GPU', 'FPGA', 'Other']
        self._allowed_record_types = ['cloud']


        RecordGenerator._get_optional_fields(self)


    def _get_record(self, keys, job_id):
        """Get a record, then add summary-specific items."""
        record = RecordGenerator._get_record(self, keys, job_id)
        record['GlobalUserName'] = get_random_string(dns)
        record['Month'] = get_random_int(end=12)
        record['Year'] = get_random_int(2000, 2021)

        #record['Type'] = mix_enums(self._allowed_types)
        #record['associatedRecordType'] = mix_enums(self._allowed_record_types)

        record['Type'] = get_random_string(self._allowed_types)
        record['AssociatedRecordType'] = get_random_string(self._allowed_record_types)

        return record


if __name__ == '__main__':
    recs_per_msg = config.defaults['records_per_message']
    no_msgs = config.defaults['number_of_messages']
    msg_dir = config.defaults['message_dir']
    msg_fmt = config.defaults['message_format']

    grg = gpu.GPURecordGenerator(recs_per_msg, no_msgs, msg_dir)
    grg.write_messages(msg_fmt)

    gsg = gpu.GPUSummaryGenerator(recs_per_msg, no_msgs, msg_dir)
    gsg.write_messages(msg_fmt)
