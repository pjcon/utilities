import os

from apel_message_generator import config
from apel_message_generator.generators import (
    job, summary, spec, blahd, event, join_job, gpu
)

class LinkedRecordGenerator():
    """ Coordinates generator classes to match specific entries.

    Note: This class uses the generator classes but is not supposed to inherit from them.
    """

    record_generators = {
        'job':job.JobRecordGenerator,
        'summary':summary.SummaryRecordGenerator,
        'gpu':gpu.GPURecordGenerator,
        'gpusummary':gpu.GPUSummaryGenerator,
        'spec':spec.SpecRecordGenerator,
        'blahd':blahd.BlahdRecordGenerator,
        'event':event.EventRecordGenerator
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
    
    def _gen_type_ind(self, gen_name):
        return self._linked_record_types.index(gen_name)

    def _init_record_generators(self):
        self._linked_record_generators = [LinkedRecordGenerator.record_generators[lr]
                                          (self._recs_per_msg, self._no_msgs, None)
                                          for lr in self._linked_record_types]

        for lri, lrg in enumerate(self._linked_record_generators):
            lrg._msg_path = os.path.abspath(os.path.join(self._msgs_root, os.path.basename(lrg._msg_path)))

    def _get_linked_record(self, job_id, record_method=None):
        """ Starting with record generator 0, make linked fields of successive records match up """

        # Generate a single record for each record type
        # e.g., [<JobRecord>, <EventRecord>, <BlahdRecord>, <SpecRecord>] 
        linked_record = [lrg._record_methods[record_method](job_id) 
                                for lrg in self._linked_record_generators]

        # Loop through linked record fields
        # For each linked record, make all entries match up,
        # starting with the first non-empty one as the 'master'.
        for record_field_tuple in self._linked_record_fields:
            master_field_chosen = False
            for fi, field in enumerate(record_field_tuple):
                if field: # If field entry is occupied, apply link
                    if not master_field_chosen: # Master field is first field
                        master_field_chosen = True
                        master_field = field
                        mfi = fi
                else: 
                    continue

                # Match up records
                lr_value = linked_record[fi][field]
                mlr_value = linked_record[mfi][master_field]

                # If types don't match in some way
                if type(lr_value) == type(mlr_value):
                    linked_record[fi][field] = mlr_value
                else:
                    print(f'ERROR: linked values <{field}={lr_value}> ({type(lr_value)}), and <{master_field}={mlr_value}> ({type(mlr_value)}) are not of the same type for\
                            \n\r{type(self._linked_record_generators[fi]).__name__} and {type(self._linked_record_generators[mfi]).__name__}')
                    exit(1)

        return linked_record

    def _generate_linked_records(self):
        # Generate a list of record lists separated by type, ready to write to messages

        # Generate a list of linked record
        _linked_record_list = [self._get_linked_record(i, record_method='full') 
                               for i in range(self._no_msgs*self._recs_per_msg)]

        # Convert inner lists from linked records to lists matched by record types
        self._linked_records = [[] for i in self._linked_record_types]
        for linked_record in _linked_record_list:
            for ri, record in enumerate(linked_record):
                self._linked_records[ri].append(record)

        return self._linked_records

    def write_messages(self, msg_fmt):

        # Create a list of 
        self._generate_linked_records()

        # Manually override write_messages process
        print(f'Creating {len(self._linked_record_generators)} record types.')
        for which_generator, generator in enumerate(self._linked_record_generators):

            # Create a record "buffer" generator object for each record generator
            # RecordGenerator._get_buffered_record(...) is called to return a record
            i = list(self._linked_records[which_generator])

            generator.load_record_buffer(self._linked_records[which_generator])
            generator.write_messages(msg_fmt, method='buffered')
 