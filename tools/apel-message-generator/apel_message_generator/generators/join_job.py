import os

from apel_message_generator import config
from apel_message_generator import utils
from apel_message_generator.generators.record_generator import RecordGenerator
from apel_message_generator.generators.linked_record_generator import LinkedRecordGenerator

class JoinJobRecordsGenerator(LinkedRecordGenerator):
    """Generate linked records for testing JoinJobRecords"""

    def __init__(self, recs_per_msg, no_msgs, msgs_root):
            
        # Job records are built from Event, Blahd, and Spec records
        record_types = ['job', 'event', 'blahd', 'spec']

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
            ('InfrastructureDescription', 'Infrastructure','',''),
            ('MemoryReal', 'MemoryReal','',''),
            ('MemoryVirtual', 'MemoryVirtual','',''),
            ('ServiceLevelType', '','','ServiceLevelType'),
            ('ServiceLevel', '','','ServiceLevel')
        ]

        super(JoinJobRecordsGenerator, self).__init__(recs_per_msg, no_msgs, msgs_root,
                                            record_types=record_types,
                                            linked_fields=linked_fields)

    def _generate_linked_records(self):
        # Account for the one-to-many mapping between SpecRecords and JobRecords
        
        linked_records = super(JoinJobRecordsGenerator, self)._generate_linked_records()

        # For each unique combination of SiteID and SubmitHostID/CEID, have a SpecRecord
        # ...
        siteid_ceid_pairs = []
        job_records = linked_records[self._gen_type_ind('job')]
        for ri, jr in enumerate(job_records):
            siteid_ceid_pairs.append((jr['SiteID'], jr['SubmitHostID']))
        siteid_ceid_pairs = list(set(siteid_ceid_pairs))

        n_pairs = len(siteid_ceid_pairs)

        spec_records = linked_records[self._gen_type_ind('spec')]
        spec_records = spec_records[:n_pairs]

        # - For those spec_records assign a unique SiteID, CEID to each from the list
        # - Using spec as the master, set the ServiceLevelType, ServiceLevel of job records
        #   with matching SiteID, SubmitHostID

        #i = 0
        for sr, pair in zip(spec_records, siteid_ceid_pairs):
            sr['SiteID'], sr['CEID'] = pair

            for jr in job_records: # TODO better algorithm
                jr_pair = jr['SiteID'], jr['SubmitHostID']
                if pair == jr_pair:
                    jr['ServiceLevelType'] = sr['ServiceLevelType']
                    jr['ServiceLevel'] = sr['ServiceLevel']
                else:
                    continue

        # Apply timing logic 

        # spec start < event end or spec start is null, and spec stop > event end
        for sr in spec_records:
            sr['StopTime'] = 'NULL' # NULL
            sr['StartTime'] = datetime.datetime.utcfromtimestamp(0).strftime('%Y-%m-%d %H:%M:%S')

        # TODO (encourage the following)
        # Blahd validfrom < event endtime
        # blahd validuntil > event endtime

        self._linked_records[self._gen_type_ind('spec')] = spec_records

        return self._linked_records


def main():
    recs_per_msg = config.defaults['records_per_message']
    no_msgs = config.defaults['number_of_messages']
    msg_dir = config.defaults['message_dir']
    msg_fmt = config.defaults['message_format']

    try:
        jjrg = JoinJobRecordsGenerator(recs_per_msg, no_msgs, msg_dir)
        jjrg.write_messages(msg_fmt)
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()