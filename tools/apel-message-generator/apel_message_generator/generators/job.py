import os

from apel_message_generator.generators.record_generator import RecordGenerator

class JobRecordGenerator(RecordGenerator):
    """Generates job record messages for testing the new APEL system."""

    def __init__(self, recs_per_msg, no_msgs, dir):
        """Call the parent constructor, then set up specific variables."""
        super(JobRecordGenerator, self).__init__(recs_per_msg, no_msgs)
        # Variables which control the operation of the generator.

        if dir is None:
            self._msg_path = "job-msgs"
        else:
            self._msg_path = dir
        self._msg_path = os.path.abspath(self._msg_path)

        self._header = "APEL-individual-job-message: v0.2"

        # Fields which are required by the message format.
        self._mandatory_fields = ["SiteID", "SubmitHostID", "LocalJobId", "WallDuration",
                    "CpuDuration", "StartTime", "EndTime", "ServiceLevelType",
                    "ServiceLevel"]

        # All fields in the standard order
        self._all_fields  = ["SiteID", "SubmitHostID", "MachineNameID", "QueueID",
                        "LocalJobId", "LocalUserId",
                       "GlobalUserNameID", "FQAN", "VOID", "VOGroupID", "VORoleID", 
                       "WallDuration", "CpuDuration", "NodeCount", "Processors", 
                       "MemoryReal", "MemoryVirtual", "StartTime", "EndTime",
                        "EndYear", "EndMonth",
                        "InfrastructureDescription", "InfrastructureType",
                       "ServiceLevelType", "ServiceLevel"]

        # Fields whose values should be integers
        self._int_fields = ["GlobalUserNameID","WallDuration", "CpuDuration", "SubmitHostID", "LocalJobId",
                      "Processors", "NodeCount", "MachineNameID",
                      "QueueID", "MemoryReal", "MemoryVirtual", "SiteID", 'VOID',
                      'VOGroupID', 'VORoleID']

        # Fields whose values should be integers
        self._float_fields = ["ServiceLevel"]

        self._datetime_fields = ['StartTime', 'EndTime']

        # Some example FQANs, some of which aren't actual FQANs.
        self._fqans = ['/atlas/higgs/Role=NULL/Capability=NULL',
                       '/cms/uscms/Role=cmsphedex',
                       '/cms/uscms',
                       '/not a real fqan',
                       'junk']

        self._factors = ['HEPSPEC', 'Si2k']

        self._field_ranges['SiteID'] = [1, 10]
        self._field_ranges['SubmitHostID'] = [1, 10]

        RecordGenerator._get_optional_fields(self)

    def _get_record(self, keys, job_id):
        """Add job-specific items to the record after calling the generic get_record() method."""
        # Call parent class method
        record = RecordGenerator._get_record(self, keys, job_id)
        record['GlobalUserName'] = get_random_string(dns)
        record['FQAN'] = get_random_string(self._fqans)
        record['LocalJobId'] = job_id
        record['ServiceLevelType'] = get_random_string(self._factors) 

        start = utc_to_timestamp(record['StartTime'])
        finish = utc_to_timestamp(record['EndTime'])

        if start > finish:
            finish = start + get_random_int(1, 1000)
            record['EndTime'] = timestamp_to_utc(finish)

        return record


if __name__ == '__main__':
    recs_per_msg = config.defaults['records_per_message']
    no_msgs = config.defaults['number_of_messages']
    msg_dir = config.defaults['message_dir']
    msg_fmt = config.defaults['message_format']

    jrg = JobRecordGenerator(recs_per_msg, no_msgs, msg_dir)
    jrg.write_messages(msg_fmt)
