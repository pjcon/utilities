import os

from apel_message_generator import config
from apel_message_generator import utils
from apel_message_generator.generators.record_generator import RecordGenerator


class SpecRecordGenerator(RecordGenerator):
    """Generator for Spec messages"""
    #SiteID            INT NOT NULL
    #CEID              INT NOT NULL
    #StartTime         DATETIME
    #StopTime          DATETIME
    #ServiceLevelType  VARCHAR(50) NOT NULL
    #ServiceLevel      DECIMAL(10,3)

    def __init__(self, recs_per_msg, no_msgs, msg_path):
        """Define constants used by the summary records."""

        super(SpecRecordGenerator, self).__init__(recs_per_msg, no_msgs)

        if msg_path is None:
            self._msg_path = "spec-msgs"
        else:
            self._msg_path = msg_path
        self._msg_path = os.path.abspath(self._msg_path)

        self._header = "Uninplemented"

        # Fields which are required by the message format.
        self._mandatory_fields = ['SiteID', 'CEID', 'ServiceLevelType']

        # All fields in the standard order
        self._all_fields = ['SiteID', 'CEID', 'StartTime', 'StopTime', 
                            'ServiceLevelType', 'ServiceLevel']

        # Fields whose values should be integers, except EarliestEndTime and LatestEndTime
        self._int_fields = ['SiteID', 'CEID']

        self._datetime_fields = ['StartTime', 'StopTime']

        # Fields whose values should be integers
        self._float_fields = ['ServiceLevel']

        RecordGenerator._get_optional_fields(self)

    def _get_record(self, keys, job_id):
        """Get a record, then add spec-specific items."""
        record = RecordGenerator._get_record(self, keys, job_id)

        start = utils.utc_to_timestamp(record['StartTime'])
        finish = utils.utc_to_timestamp(record['StopTime'])

        if start > finish:
            record['StopTime'], record['StartTime'] = utils.timestamp_to_utc(start), utils.timestamp_to_utc(finish)

        return record

def main():
    recs_per_msg = config.defaults['records_per_message']
    no_msgs = config.defaults['number_of_messages']
    msg_dir = config.defaults['message_dir']
    msg_fmt = config.defaults['message_format']

    try:
        srg = SpecRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        srg.write_messages(msg_fmt)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()