#!/bin/env python3
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
import json

# Arbitrary strings for filling messages.
sample_strings = '''Site Navigation
Home
Latest Information
Email Hoaxes
Internet Scams
Previous Issues
Site FAQs
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
            self._no_msgs = 100
        else:
            self._no_msgs = no_msgs
        self._msg_path = ''

        self._header = ''
        self._mandatory_fields = []
        self._all_fields = []
        self._int_fields = []
        self._float_fields = []

        self._optional_fields = []

        # Label formats for message output
        self._ordered_message_formats = {
            None: self.get_apel_message_ordered,
            'apel': self.get_apel_message_ordered,
            'csv': self.get_csv_message_ordered,
            'json': self.get_json_message_ordered
        }

        # Label records for message output
        self._record_methods = {
            None: self._get_full_record,
            'full': self._get_full_record,
            'minimal': self._get_minimal_record,
            'valid_none': self._get_valid_none_record,
            'incomplete': self._get_incomplete_record,
            'buffered': self._get_buffered_record
        }

        self._get_record_method = 'valid_none'
        self.buffered_records = (None)


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
                record[key] = get_random_int()
            elif key in self._float_fields:
                record[key] = get_random_float()
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

    def _get_buffered_record(self, *args):
        # Allow records to be set externally
        try:
            return next(self.buffered_records)
        except Exception as e:
            print('ERROR: misconfigured record buffer.')
            raise e

    def get_apel_message(self, prefix, record_method='full'):
        """Get a valid message string."""
        message = self._header + "\n"
        for i in range(self._recs_per_msg):
            dict = self._record_methods[record_method](prefix + str(i)) # TODO rejecting based on single record failing.
            for key in dict.keys():
                message += key
                message += ": "
                message += dict[key]
                message += "\n"
            message += "%%\n"
        return message

    def get_json_message(self, prefix, record_method='full'):
        """ Get valid json message as string."""

        JSON_MSG_DICT = dict(Type=self._msg_type, Version=self._msg_version, 
                             UsageRecords=[])

        for i in range(self._recs_per_msg):
            record = self._record_methods[record_method](prefix + str(i))
            JSON_MSG_DICT['UsageRecords'].append(record)

        return str(JSON_MSG_DICT).replace('"', "'")

    def get_apel_message_ordered(self, prefix, record_method='full'):
        """Get a valid message string, with its fields in the correct order."""
        message = self._header + "\n"
        for i in range(self._recs_per_msg):
            dict = self._record_methods[record_method](prefix + str(i))
            # go through in the order of all_fields
            for key in self._all_fields:
                if key in dict.keys():
                    message += key
                    message += ": "
                    message += str(dict[key])
                    message += "\n"
            message += "%%\n"
        return message

    def get_json_message_ordered(self, prefix, record_method='full'):
        """ Get valid json message as string, with its fields in the correct order."""

        JSON_MSG_DICT = dict(Type=self._msg_type, Version=self._msg_version, 
                             UsageRecords=[])

        for i in range(self._recs_per_msg):
            record = self._record_methods[record_method](prefix + str(i))
            # TODO record = self._get_valid_none_record(prefix + str(i))
            record_ordered = {key: record[key] for key in self._all_fields if key in record}
            JSON_MSG_DICT['UsageRecords'].append(record_ordered)

        #return str(JSON_MSG_DICT).replace("'", '"')
        return json.dumps(JSON_MSG_DICT, indent=4).replace("'", '"')

    def get_apel_message_lowercase(self, prefix, record_method='full'):
        """Get a message with its key-value pairs in lower-case."""
        message = self._header + "\n"
        for i in range(self._recs_per_msg):
            dict = self._record_methods[record_method](prefix + str(i))
            for key in dict.keys():
                message += key.lower()
                message += ": "
                message += str(dict[key]).lower()
                message += "\n"
            message += "%%\n"
        return message

    def get_csv_message_ordered(self, prefix, record_method='full'):
        """Get a valid csv string, with its columns in the correct order."""
        message = '# ' + self._header + "\n"

        message += '# Type: csv\n'

        dict = self._record_methods[record_method](prefix + '0')
        message += ','.join([k for k in self._all_fields if k in dict]) + '\n'

        delim = ','

        for i in range(self._recs_per_msg-1):
            dict = self._record_methods[record_method](prefix + str(i))

            message_list = [str(dict[k]) for k in self._all_fields if k in dict]
            
            # Check if delim already exists within message
            if [m for m in message_list if delim in m]:
                save = delim
                delim = '\t' # Choose alternative delimiter
                message.replace(save, delim)
            
            message += delim.join(message_list) + '\n'

        return message

    def get_json_message_lowercase(self, prefix, record_method='full'):
        """Get a json message with its key-value pairs in lower-case."""

        JSON_MSG_DICT = dict(Type=self._msg_type, Version=self._msg_version, 
                             UsageRecords=[])

        for i in range(self._recs_per_msg):
            record = self._record_methods[record_method](prefix + str(i))
            record_ordered = {key.lower(): record[key].lower() for key in self._all_fields if key in record}
            JSON_MSG_DICT['UsageRecords'].append(record_ordered)

        return json.dumps(JSON_MSG_DICT, indent=4)

    def write_messages(self, fmt=None, method=None):
        """Write the specified number of messages to the specified directory."""

        print(f"Creating {str(self._no_msgs)} messages with {str(self._recs_per_msg)} records each\
 of type {type(self).__name__}.")

        if fmt not in self._ordered_message_formats:
            print(f'ERROR: Format {fmt} not available.')
            print(f'Valid formats include: {list(self._ordered_message_formats.keys())}')
            exit(1)

        if not os.path.exists(self._msg_path):
            print("Creating directory:", self._msg_path, "...")
            os.makedirs(self._msg_path)

        print("Writing to directory:", self._msg_path, "...")

        for i in range(self._no_msgs):
            prefix = get_prefix(i)
            filepath = os.path.join(self._msg_path, str(i).zfill(14))
            f = open(filepath, 'w')
            f.write(self._ordered_message_formats[fmt](prefix, record_method=method))
            f.close()

        print("Done.")


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
        self._mandatory_fields = ["SiteID", "SubmitHost", "LocalJobId", "WallDuration",
                    "CpuDuration", "StartTime", "EndTime", "ServiceLevelType",
                    "ServiceLevel"]

        # All fields in the standard order
        self._all_fields  = ["SiteID", "SubmitHost", "LocalJobId", "LocalUserId",
                       "GlobalUserName", "FQAN", "WallDuration", "CpuDuration",
                       "Processors", "NodeCount", "StartTime", "EndTime",
                       "MemoryReal", "MemoryVirtual", "ServiceLevelType",
                       "ServiceLevel"]

        # Fields whose values should be integers
        self._int_fields = ["WallDuration", "CpuDuration",
                      "Processors", "NodeCount", "StartTime", "EndTime",
                      "MemoryReal", "MemoryVirtual", "SiteID"]

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
            record['EndTime'] = record['StartTime'] + get_random_int(1, 1000)

        return record


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

        # Fields whose values should be integers
        self._float_fields = ['ServiceLevel']

        RecordGenerator._get_optional_fields(self)


    def _get_record(self, keys, job_id):
        """Get a record, then add spec-specific items."""
        record = RecordGenerator._get_record(self, keys, job_id)
        """
        [ ] Fill datetime fields starttime, stoptime
        [ ] Ensure StartTime < EndTime
        [ ] Fulfill requirements of JoinJobRecords
            - SpecRecords.StopTime > EventRecords.EndTime, or StopTime is NULL
            - SpecRecords.StartTime <= EventRecords.EndTime
            - SpecRecords.SiteID = EventRecords.SiteID
        """


        return record


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
        self._all_fields = ['GlobalUserNameID', 'FQAN', 'VOID', 'VOGroupID', 'VORoleID',
                            'CEID', 'GlobalJobId', 'LrmsId', 'SiteID', 'ValidFrom',
                            'ValidUntil', 'Processed']

        # Fields which are required by the message format.
        self._mandatory_fields = ['GlobalUserNameID', 'VOID', 'VOGroupID', 'VORoleID',
                                    'CEID', 'SiteID']

        # Fields whose values should be integers, except EarliestEndTime and LatestEndTime
        self._int_fields = ['VOID', 'VOGroupID', 'VORoleID', 'CEID', 'SiteID', 'Processed']

        # Fields whose values should be integers
        self._float_fields = []

        RecordGenerator._get_optional_fields(self)

    def _get_record(self, keys, job_id):
        """Get a record, then add blahd-specific items."""
        record = RecordGenerator._get_record(self, keys, job_id)

        """ TODO Blahd specific requirements
        [ ] Fill datetime fields ValidFrom, ValidUntil
        [ ] Processed is either 1 or 0 (or maybe 2)
        """
        return record


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
                            'MemoryReal', 'MemoryVirtual', 'Processors', 'NodeCount', 'Status']

        # Fields whose values should be integers
        self._float_fields = []

        RecordGenerator._get_optional_fields(self)

    def _get_record(self, keys, job_id):
        """Get a record, then add blahd-specific items."""
        record = RecordGenerator._get_record(self, keys, job_id)

        """ TODO Event specific requirements
        [ ] Fill datetime fields StartTime, EndTime
        [ ] Status is either 0, 1, 2
        """
        return record


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

        self._msg_type = "APEL GPU message"
        self._msg_version = "0.1"

        # Fields which are required by the message format.
        self._mandatory_fields = [
            "SiteName", "GlobalUserName", "FQAN",
            "Count", "Cores", "AvailableDuration", "Type", "Model",
            "AssociatedRecordType", "ActiveDuration", "MeasurementYear",
            "MeasurementMonth", 
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


class LinkedRecordGenerator():

    record_generators = {
        'job':JobRecordGenerator,
        'summary':SummaryRecordGenerator,
        'gpu':GPURecordGenerator,
        'spec':SpecRecordGenerator,
        'blahd':BlahdRecordGenerator,
        'event':EventRecordGenerator
    }

    def __init__(self, recs_per_msg, no_msgs, msgs_root,
                 record_types=[], linked_fields=[]):

        #super(LinkedRecordGenerator, self).__init__(recs_per_msg, no_msgs)

        self._linked_record_types = record_types
        self._linked_record_fields = linked_fields

        if msgs_root is None:
            self._msgs_root = './linked-msgs'
        else:
            self._msgs_root = msgs_root

        if recs_per_msg is None:
            self._recs_per_msg = 100
        else:
            self._recs_per_msg = recs_per_msg

        if no_msgs is None:
            self._no_msgs = 100
        else:
            self._no_msgs = no_msgs

        for lr in self._linked_record_types:
            if lr not in LinkedRecordGenerator.record_generators:
                raise ValueError(f'{lr} not present in available record generators:\n{list(records.keys())}')

        self._init_record_generators()


    def _init_record_generators(self):
        """ DRAFT
        Decide which fields will depend on fields from another record type.

        Fields list of tuples of dependent fields between each type in RecordTypes.

        e.g.
        fields = [('SiteID', 'SiteID', ''), ('CEID', '', 'CEID')]
        RecordTypes = [SpecRecordGenerator, JobRecordGenerator, BlahdRecordGenerator]
        - Can lead to dependency cycles/non-resolution: Generate JobRecords first.
        
        """

        self._linked_record_generators = [LinkedRecordGenerator.record_generators[lr]
                                          (self._recs_per_msg, self._no_msgs, None)
                                          for lr in self._linked_record_types]

        for lri, lrg in enumerate(self._linked_record_generators):
            lrg._msg_path = os.path.join(self._msgs_root, lrg._msg_path)


    def _get_linked_records(self, job_id, record_method='full'):
        """ Starting with record generator 0, make linked fields of successive records match up """

        # Generate a single record for each record type
        self._linked_records = [lrg._record_methods[record_method](job_id) 
                                for lrg in self._linked_record_generators]

        for li, tlr in enumerate(self._linked_records):
            # Keys for this record, the li'th entry in each field element
            # relates to this generator

            if li == 0:
                # Keys for master record
                linked_keys = [rf[li] for rf in self._linked_record_fields]
                continue

            # Amend latter records to match master record
            this_linked_keys = [rf[li] for rf in self._linked_record_fields]

            for lk, tlk in zip(linked_keys, this_linked_keys):
                if not tlk: # If no key listed for this record, no changes needed
                    continue

                lr_value = self._linked_records[0][lk]
                tlr_value = tlr[tlk]

                if type(lr_value) == type(tlr_value):
                    tlr[tlk] = lr_value
                else:
                    print(f'ERROR: linked values <{tlr_value}>, and <{lr_value}> are not of the same type for\
                            \n\r{self._linked_record_generators[li+1]} and {self._linked_record_generators[0]}')
                    exit(1)

        return self._linked_records


    def write_messages(self, msg_fmt):

        , '','',''# Generate a set of linked records for each message
        linked_records = [self._get_linked_records(i, record_method='full') 
                          for i in range(self._no_msgs*self._recs_per_msg)]
        
        n_types = len(linked_records[0])
        print(f'Creating {n_types} record types.')
        for li, lrg in enumerate(self._linked_record_generators):

            # Create a record "buffer" generator object for each record generator
            # RecordGenerator._get_buffered_record(...) is called to return a record
            lrg.buffered_records = (lr[li] for lr in linked_records)
            lrg.write_messages(msg_fmt, method='buffered')

        

class JoinJobRecordsGenerator(LinkedRecordGenerator):
    """Generate linked records for testing JoinJobRecords"""

    def __init__(self, recs_per_msg, no_msgs, msgs_root):
            
        # Job records are built from Event, Blahd, and Spec records
        record_types = ['job'] + ['event', 'blahd']

        # How are job fields linked to latter type fields?
        linked_fields = [
            ('SiteID', 'SiteID', 'SiteID', 'SiteID'),
            ('SubmitHostID', '','CEID','CEID'),
            ('MachineNameID', 'MachineNameID','',''),
            ('QueueID', 'QueueID','',''),
            ('LocalJobId', 'JobName','LrmsId',''),
            ('LocalUserId', 'LocalUserID','',''),
            ('GlobalUserNameID', '','GlobalUserNameID',''),
            ('FQAN', '','FQAN',''),
            ('VOID', '','VOID',''),
            ('VOGroupID', '','VOGroupID',''),
            ('VORoleID', '','VORoleID',''),
            ('WallDuration', 'WallDuration','',''),
            ('CpuDuration', 'CpuDuration','',''),
            ('Processors', 'Processors','',''),
            ('NodeCount', 'NodeCount','',''),
            ('StartTime', 'StartTime','',''),
            ('EndTime', 'EndTime','',''),
            ('InfastructureDescription', 'Infrastructure','',''),
            ('MemoryRead', 'MemoryReal','',''),
            ('MemoryVirtual', 'MemoryVirtual','',''),
            ('ServiceLevelType', 'ServiceLevelType','',''),
            ('ServiceLevel', 'ServiceLevel','','')
        ]

        super(JoinJobRecordsGenerator, self).__init__(recs_per_msg, no_msgs, msgs_root,
                                            record_types=record_types,
                                            linked_fields=linked_fields)


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

def mix_enums(enums):
    # Balance mix correct enums with bad strings
    samples = [get_random_string(sample_strings) for i in range(len(enums))]
    enum_mix = enums + samples
    return get_random_string(enum_mix)


def usage():
    """Print a usage message."""
    print("Usage: " + sys.argv[0] + \
        """ [-r <recs-per-msg> -m <no-msgs> -d <directory> -f <format>] jobs|summaries|gpu

         Defaults: recs-per-msg: 1000
                   no-msgs:      100
                   directory:    ./job-msgs | ./sum-msgs
                   format:       apel (csv)
        """)


if __name__ == '__main__':
    """Parse the command-line arguments and create the appropriate type of
    message."""

    recs_per_msg = None
    no_msgs = None
    msg_dir = None
    msg_fmt = None

    opts = None
    args = None

    try:
        opts, args = getopt.getopt(sys.argv[1:], "r:m:d:f:")

    except getopt.GetoptError as e:
        print("Invalid options.")
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
            elif o == "-f":
                msg_fmt = a
    except ValueError:
        print("Invalid options.")
        usage()
        sys.exit()

    allowed_args = [
        "jobs", "summaries", "gpu", "event", "blahd", "spec", "join-job-records"
    ]

    if "jobs" in args:
        jrg = JobRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        jrg.write_messages(msg_fmt)
    elif "summaries" in args:
        srg = SummaryRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        srg.write_messages(msg_fmt)
    elif "gpu" in args:
        grg = GPURecordGenerator(recs_per_msg, no_msgs, msg_dir)
        grg.write_messages(msg_fmt)
    elif "blahd" in args:
        brg = BlahdRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        brg.write_messages(msg_fmt)
    elif "event" in args:
        erg = EventRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        erg.write_messages(msg_fmt)
    elif "spec" in args:
        srg = SpecRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        srg.write_messages(msg_fmt)
    elif "join-job-records" in args:
        jjrg = JoinJobRecordsGenerator(recs_per_msg, no_msgs, msg_dir)
        jjrg.write_messages(msg_fmt)

    else:
        print(f"Argument must be one of: {', '.join(allowed_args)}.")
        usage()
        sys.exit()
