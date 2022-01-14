import os

from apel_message_generator import config
from apel_message_generator import utils
from apel_message_generator.generators.record_generator import RecordGenerator


class EventRecordGenerator(RecordGenerator):
    """Generator for Event messages"""
    #SiteID          INT             NOT NULL
    #JobName         VARCHAR(60)     NOT NULL
    #LocalUserID     VARCHAR(20)        
    #LocalUserGroup  VARCHAR(20)
    #WallDuration    INT
    #CpuDuration     INT
    #StartTime       DATETIME        NOT NULL
    #EndTime         DATETIME        NOT NULL
    #Infrastructure  VARCHAR(100)
    #MachineNameID   INT             NOT NULL
    #QueueID         INT             NOT NULL
    #MemoryReal      BIGINT
    #MemoryVirtual   BIGINT
    #Processors      INT
    #NodeCount       INT
    #Status          INT

    def __init__(self, recs_per_msg, no_msgs, msg_path):
        """Define constants used by the summary records."""

        super(EventRecordGenerator, self).__init__(recs_per_msg, no_msgs)

        if msg_path is None:
            self._msg_path = "event-msgs"
        else:
            self._msg_path = msg_path
        self._msg_path = os.path.abspath(self._msg_path)

        self._header = "Uninplemented"

        # Fields which are required by the message format.
        self._mandatory_fields = ['SiteID', 'JobName', 'StartTime', 'EndTime',
                                  'MachineNameID', 'QueueID']

        # All fields in the standard order
        self._all_fields = ['SiteID', 'JobName', 'LocalUserID', 'LocalUserGroup', 'WallDuration',
                            'CpuDuration', 'StartTime', 'EndTime', 'Infrastructure', 'MachineNameID',
                            'QueueID', 'MemoryReal', 'MemoryVirtual', 'Processors', 'NodeCount', 
                            'Status']

        # Fields whose values should be integers, except EarliestEndTime and LatestEndTime
        self._int_fields = ['SiteID', 'WallDuration', 'CpuDuration', 'MachineNameID', 'QueueID', 
                            'MemoryReal', 'MemoryVirtual', 'Processors', 'NodeCount', 'Status',
                            'JobName']

        self._datetime_fields = ['StartTime', 'EndTime']

        # Fields whose values should be integers
        self._float_fields = []

        self._field_ranges['Status'] = [0, 2]

        RecordGenerator._get_optional_fields(self)

    def _get_record(self, keys, job_id):
        """Get a record, then add blahd-specific items."""
        record = RecordGenerator._get_record(self, keys, job_id)

        start = utils.utc_to_timestamp(record['StartTime'])
        finish = utils.utc_to_timestamp(record['EndTime'])

        if start > finish:
            record['EndTime'], record['StartTime'] = utils.timestamp_to_utc(start),  utils.timestamp_to_utc(finish)

        return record

def main():
    recs_per_msg = config.defaults['records_per_message']
    no_msgs = config.defaults['number_of_messages']
    msg_dir = config.defaults['message_dir']
    msg_fmt = config.defaults['message_format']

    try:
        erg = EventRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        erg.write_messages(msg_fmt)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()