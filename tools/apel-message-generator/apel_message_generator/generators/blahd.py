import os

from apel_message_generator.generators.record_generator import RecordGenerator


class BlahdRecordGenerator(RecordGenerator):
    """Generator for Blahd messages"""
    #GlobalUserNameID    int             not null
    #FQAN                varchar(255)    null
    #VOID                int             not null
    #VOGroupID           int             not null
    #VORoleID            int             not null
    #CEID                int             not null
    #GlobalJobId         varchar(255)    null
    #LrmsId              varchar(255)    null
    #SiteID              int             not null
    #ValidFrom           datetime        null
    #ValidUntil          datetime        null    
    #Processed           int             null

    def __init__(self, recs_per_msg, no_msgs, msg_path):
        """Define constants used by the summary records."""

        super(BlahdRecordGenerator, self).__init__(recs_per_msg, no_msgs)

        if msg_path is None:
            self._msg_path = "blahd-msgs"
        else:
            self._msg_path = msg_path
        self._msg_path = os.path.abspath(self._msg_path)

        self._header = "Uninplemented"

        # All fields in the standard order
        self._all_fields = ['TimeStamp', 'GlobalUserNameID', 'FQAN', 'VOID', 'VOGroupID', 'VORoleID',
                            'CEID', 'GlobalJobId', 'LrmsId', 'SiteID', 'ValidFrom', 
                            'ValidUntil', 'Processed']

        # Fields which are required by the message format.
        self._mandatory_fields = ['GlobalUserNameID', 'VOID', 'VOGroupID', 'VORoleID',
                                    'CEID', 'SiteID']

        # Fields whose values should be integers, except EarliestEndTime and LatestEndTime
        self._int_fields = ['GlobalUserNameID', 'VOID', 'VOGroupID', 'VORoleID', 'CEID', "LrmsId",
                             'SiteID', 'Processed']

        self._datetime_fields = ['TimeStamp', 'ValidFrom', 'ValidUntil']

        self._field_ranges['Processed'] = [0, 0] # TODO this should normally be 0, 1, but leave 0, 0 for now

        # Fields whose values should be integers
        self._float_fields = []

        RecordGenerator._get_optional_fields(self)

    def _get_record(self, keys, job_id):
        """Get a record, then add blahd-specific items."""
        record = RecordGenerator._get_record(self, keys, job_id)

        start = utc_to_timestamp(record['ValidFrom'])
        finish = utc_to_timestamp(record['ValidUntil'])

        if start > finish:
            record['ValidUntil'], record['ValidFrom'] = timestamp_to_utc(start), timestamp_to_utc(finish)

        return record


if __name__ == '__main__':
    recs_per_msg = config.defaults['records_per_message']
    no_msgs = config.defaults['number_of_messages']
    msg_dir = config.defaults['message_dir']
    msg_fmt = config.defaults['message_format']

    brg = BlahdRecordGenerator(recs_per_msg, no_msgs, msg_dir)
    brg.write_messages(msg_fmt)
