import os
import datetime
from time import time, mktime

from apel_message_generator import config
from apel_message_generator import utils
from apel_message_generator.generators.record_generator import RecordGenerator


class SummaryRecordGenerator(RecordGenerator):
    """Generator for summary messages, defining parts specific to these."""

    def __init__(self, recs_per_msg, no_msgs, msg_path):
        """Define constants used by the summary records."""

        super(SummaryRecordGenerator, self).__init__(recs_per_msg, no_msgs)

        if msg_path is None:
            self._msg_path = "summary-msgs"
        else:
            self._msg_path = msg_path
        self._msg_path = os.path.abspath(self._msg_path)

        self._header = "APEL-summary-job-message: v0.2"

        # Fields which are required by the message format.
        self._mandatory_fields = ["Site", "Month", "Year", "WallDuration",
                "CpuDuration", "NormalisedWallDuration", "NormalisedCpuDuration",
                "NumberOfJobs"]

        # All fields in the standard order
        self._all_fields = ["Site", "Month", "Year", "GlobalUserName", "Group", "VOGroup",
                       "VORole", "EarliestEndTime", "LatestEndTime", "WallDuration", "CpuDuration", "NormalisedWallDuration",
                       "NormalisedCpuDuration", "NumberOfJobs"]

        # Fields whose values should be integers, except EarliestEndTime and LatestEndTime
        self._int_fields = ["Month", "Year", "WallDuration", "CpuDuration",
                      "NormalisedWallDuration", "NormalisedCpuDuration",
                      "NumberOfJobs"]

        # Fields whose values should be integers
        self._float_fields = []

        RecordGenerator._get_optional_fields(self)

    def _get_record(self, keys, job_id):
        """Get a record, then add summary-specific items."""
        record = RecordGenerator._get_record(self, keys, job_id)
        record['GlobalUserName'] = utils.get_random_string(config.dns)
        record['Month'] = str(utils.get_random_int(end=12))
        record['Year'] = str(utils.get_random_int(2000, 2010))

        # The rest of this method is to get EarliestEndTime and
        # LatestEndTime to fall within the correct month.
        month_start = datetime.datetime(int(record['Year']),
                                        int(record['Month']), 1)
        month_end = month_start + datetime.timedelta(28)

        start_epoch = mktime(month_start.timetuple())
        end_epoch = mktime(month_end.timetuple())

        rnd_epoch1 = utils.get_random_int(start_epoch, end_epoch)
        rnd_epoch2 = utils.get_random_int(start_epoch, end_epoch)

        if rnd_epoch1 > rnd_epoch2:
            record['EarliestEndTime'] = str(rnd_epoch2)
            record['LatestEndTime'] = str(rnd_epoch1)
        else:
            record['EarliestEndTime'] = str(rnd_epoch1)
            record['LatestEndTime'] = str(rnd_epoch2)
        return record


def main():
    recs_per_msg = config.defaults['records_per_message']
    no_msgs = config.defaults['number_of_messages']
    msg_dir = config.defaults['message_dir']
    msg_fmt = config.defaults['message_format']

    try:
        srg = SummaryRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        srg.write_messages(msg_fmt)
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()