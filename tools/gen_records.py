"""Module for generating test messages for the new APEL system.

It will currently generate job messages or summary messages, depending on
command-line arguments, but shouldn't be difficult to extend to other types of
message.  Create a subclass of RecordGenerator for each message type.
"""

import getopt
import os
import random
import sys
import datetime
from time import mktime

# Arbitrary strings for filling messages.
sample_strings = '''Site Navigation
Home
Latest Information
Email Hoaxes
Internet Scams
Previous Issues
Site FAQ's
Hoax-Slayer Social
HS About
Privacy Policy
HS Site Map
True Emails
Virus Hoaxes
Giveaway Hoaxes
Charity Hoaxes
Bogus Warnings
Email Petitions
Chain Letters
Unsubstantiated
Missing Child Hoaxes'''.splitlines()

# Some example DNs
dns = ['/C=UK/O=eScience/OU=CLRC/L=RAL/CN=apel-dev.esc.rl.ac.uk/emailAddress=sct-certificates@stfc.ac.uk',
       '/C=UK/O=eScience/OU=CLRC/L=RAL/CN=apel-consumer2.esc.rl.ac.uk/emailAddress=sct-certificates@stfc.ac.uk',
       '/c=cy/o=cygrid/o=hpcl/cn=mon101.grid.ucy.ac.cy',
       '/c=hu/o=niif ca/ou=grid/ou=niif/cn=host/egi1.grid.niif.hu',
       '/dc=org/dc=balticgrid/ou=mif.vu.lt/cn=host/grid9.mif.vu.lt',
       '/dc=es/dc=irisgrid/o=pic/cn=mon01.pic.es',
       '/dc=ro/dc=romaniangrid/o=ifin-hh/cn=tbit03.nipne.ro']

# Possible acceptable values representing null
null_values = ['NULL', 'Null', 'null', 'NONE', 'None', 'none', '']


class RecordGenerator(object):
    """Don't create a RecordGenerator object - create a subclass
    which defines the appropriate header, mandatory and optional
    fields etc."""

    def __init__(self, recs_per_msg, no_msgs):

        if recs_per_msg is None:
            self._recs_per_msg = 100
        else:
            self._recs_per_msg = recs_per_msg
        if no_msgs is None:
            self._no_msgs = 1000
        else:
            self._no_msgs = no_msgs
        self._msg_path = ''

        self._header = ''
        self._mandatory_fields = []
        self._all_fields = []
        self._int_fields = []
        self._float_fields = []

        self._optional_fields = []

    def _get_optional_fields(self):

        # optional fields = all fields - mandatory fields
        self._optional_fields = [s for s in self._all_fields]
        for item in self._mandatory_fields:
            self._optional_fields.remove(item)

    def _get_record(self, keys, job_id):
        """Get a record with all of the keys listed in keys."""
        record = {}
        for key in keys:
            if key in self._int_fields:
                record[key] = str(get_random_int())
            elif key in self._float_fields:
                record[key] = str(get_random_float())
            else:
                record[key] = get_random_string(sample_strings)
        record['job_id'] = job_id

        return record

    def _get_full_record(self, job_id):
        """Get a record string with all possible fields."""
        return self._get_record(self._all_fields, job_id)

    def _get_minimal_record(self, job_id):
        """Get a record string with only the necessary fields."""
        return self._get_record(self._mandatory_fields, job_id)

    def _get_incomplete_record(self, job_id):
        """Get a record without one of the mandatory fields."""
        # copy the list
        all_keys = [s for s in self._all_fields]
        # remove a mandatory item
        to_remove = get_random_string(self._mandatory_fields)
        all_keys.remove(to_remove)

        return self._get_record(all_keys, job_id)

    def _get_valid_none_record(self, job_id):
        """Get a record giving one of the optional fields null values."""
        rec_dict = self._get_record(self._all_fields, job_id)

        to_edit = get_random_string(self._optional_fields)
        rec_dict[to_edit] = get_random_string(null_values)

        return rec_dict

    def get_message(self, prefix):
        """Get a valid message string."""
        message = self._header + "\n"
        for i in range(self._recs_per_msg):
            dict = self._get_valid_none_record(prefix + str(i))
            for key in dict.keys():
                message += key
                message += ": "
                message += dict[key]
                message += "\n"
            message += "%%\n"
        return message

    def get_message_ordered(self, prefix):
        """Get a valid message string, with its fields in the correct order."""
        message = self._header + "\n"
        for i in range(self._recs_per_msg):
            dict = self._get_valid_none_record(prefix + str(i))
            # go through in the order of all_fields
            for key in self._all_fields:
                if key in dict.keys():
                    message += key
                    message += ": "
                    message += dict[key]
                    message += "\n"
            message += "%%\n"
        return message

    def get_message_lowercase(self, prefix):
        """Get a message with its keys in lower-case."""
        message = self._header + "\n"
        for i in range(self._recs_per_msg):
            dict = self._get_valid_none_record(prefix + str(i))
            for key in dict.keys():
                message += key.lower()
                message += ": "
                message += dict[key].lower()
                message += "\n"
            message += "%%\n"
        return message

    def write_messages(self):
        """Write the specified number of messages to the specified directory."""
        if not os.path.exists(self._msg_path):
            print "Creating directory: " + self._msg_path + "..."
            os.makedirs(self._msg_path)

        print "Writing to directory " + self._msg_path + "..."

        for i in range(self._no_msgs):
            prefix = get_prefix(i)
            filepath = os.path.join(self._msg_path, str(i).zfill(14))
            f = open(filepath, 'w')
            f.write(self.get_message_ordered(prefix))
            f.close()

        print "Done."


class JobRecordGenerator(RecordGenerator):
    """Generates job record messages for testing the new APEL system."""

    def __init__(self, recs_per_msg, no_msgs, dir):
        """Call the parent constructor, then set up specific variables."""
        super(JobRecordGenerator, self).__init__(recs_per_msg, no_msgs)
        # Variables which control the operation of the generator.

        if msg_dir is None:
            self._msg_path = "job-msgs"
        else:
            self._msg_path = msg_dir
        self._msg_path = os.path.abspath(self._msg_path)

        print "Creating " + str(self._no_msgs) + " messages of " + str(self._recs_per_msg) + " records each."

        self._header = "APEL-individual-job-message: v0.2"

        # Fields which are required by the message format.
        self._mandatory_fields = ["Site", "SubmitHost", "LocalJobId", "WallDuration",
                    "CpuDuration", "StartTime", "EndTime", "ServiceLevelType",
                    "ServiceLevel"]

        # All fields in the standard order
        self._all_fields  = ["Site", "SubmitHost", "LocalJobId", "LocalUserId",
                       "GlobalUserName", "FQAN", "WallDuration", "CpuDuration",
                       "Processors", "NodeCount", "StartTime", "EndTime",
                       "MemoryReal", "MemoryVirtual", "ServiceLevelType",
                       "ServiceLevel"]

        # Fields whose values should be integers
        self._int_fields = ["WallDuration", "CpuDuration",
                      "Processors", "NodeCount", "StartTime", "EndTime",
                      "MemoryReal", "MemoryVirtual"]

        # Fields whose values should be integers
        self._float_fields = ["ServiceLevel"]

        # Some example FQANs, some of which aren't actual FQANs.
        self._fqans = ['/atlas/higgs/Role=NULL/Capability=NULL',
                       '/cms/uscms/Role=cmsphedex',
                       '/cms/uscms',
                       '/not a real fqan',
                       'junk']

        self._factors = ['HEPSPEC', 'Si2k']

        RecordGenerator._get_optional_fields(self)

    def _get_record(self, keys, job_id):
        """Add job-specific items to the record after calling the generic get_record() method."""
        # Call parent class method
        record = RecordGenerator._get_record(self, keys, job_id)
        record['GlobalUserName'] = get_random_string(dns)
        record['FQAN'] = get_random_string(self._fqans)
        record['LocalJobId'] = job_id
        record['ServiceLevelType'] = get_random_string(self._factors)

        if int(record['StartTime']) > int(record['EndTime']):
            record['EndTime'] = record['StartTime'] + str(get_random_int(1, 1000))

        return record


class SummaryRecordGenerator(RecordGenerator):
    """Generator for summary messages, defining parts specific to these."""

    def __init__(self, recs_per_msg, no_msgs, msg_path):
        """Define constants used by the summary records."""

        super(SummaryRecordGenerator, self).__init__(recs_per_msg, no_msgs)

        if msg_dir is None:
            self._msg_path = "summary-msgs"
        else:
            self._msg_path = msg_dir
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
        record['GlobalUserName'] = get_random_string(dns)
        record['Month'] = str(get_random_int(end=12))
        record['Year'] = str(get_random_int(2000, 2010))

        # The rest of this method is to get EarliestEndTime and
        # LatestEndTime to fall within the correct month.
        month_start = datetime.datetime(int(record['Year']),
                                        int(record['Month']), 1)
        month_end = month_start + datetime.timedelta(28)

        start_epoch = mktime(month_start.timetuple())
        end_epoch = mktime(month_end.timetuple())

        rnd_epoch1 = get_random_int(start_epoch, end_epoch)
        rnd_epoch2 = get_random_int(start_epoch, end_epoch)

        if rnd_epoch1 > rnd_epoch2:
            record['EarliestEndTime'] = str(rnd_epoch2)
            record['LatestEndTime'] = str(rnd_epoch1)
        else:
            record['EarliestEndTime'] = str(rnd_epoch1)
            record['LatestEndTime'] = str(rnd_epoch2)
        return record


def get_random_int(start=1, end=1000000):
    """Get an random integer between start and end inclusive."""
    x = random.random()
    i = int(x*(end + 1 - start) + start)
    return i


def get_random_float():
    """Get a random float."""
    x = random.random()
    return x * 1000


def get_random_string(strings):
    """Get one of a list of strings at random."""
    x = random.random()
    i = int(x * len(strings))
    return strings[i]


def get_prefix(i):
    prefix = ''
    letters = 'abcdefghij'
    no_str = str(i)
    for number in no_str:
        prefix += letters[int(number)]
        return prefix


def usage():
    """Print a usage message."""
    print "Usage: " + sys.argv[0] + \
        """ [-r <recs-per-msg> -m <no-msgs> -d <directory>] jobs|summaries

         Defaults: recs-per-msg: 1000
                   no-msgs:      100
                   directory:    ./job-msgs | ./sum-msgs
        """


if __name__ == '__main__':
    """Parse the command-line arguments and create the appropriate type of
    message."""

    recs_per_msg = None
    no_msgs = None
    msg_dir = None

    opts = None
    args = None

    try:
        opts, args = getopt.getopt(sys.argv[1:], "r:m:d:")

    except getopt.GetoptError, e:
        print "Invalid arguments."
        usage()
        sys.exit()

    try:
        for o, a in opts:
            if o == "-r":
                recs_per_msg = int(a)
            elif o == "-m":
                no_msgs = int(a)
            elif o == "-d":
                msg_dir = a
    except ValueError:
        print "Invalid arguments."
        usage()
        sys.exit()

    if "jobs" in args:
        jrg = JobRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        jrg.write_messages()
    elif "summaries" in args:
        srg = SummaryRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        srg.write_messages()
    else:
        print "Neither job nor summary records specified."
        usage()
        sys.exit()
