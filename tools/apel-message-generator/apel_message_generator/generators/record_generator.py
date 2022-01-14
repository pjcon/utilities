import os
import json

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
        self._datetime_fields = []

        self._field_ranges = {} # Map field to a range # TODO generalise to dns etc

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
        self._buffered_records = [None]


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
                if key in self._field_ranges:
                    lower = self._field_ranges[key][0]
                    upper = self._field_ranges[key][1]
                else:
                    lower = 0
                    upper = 1000000

                record[key] = get_random_int(lower, upper)

            elif key in self._float_fields:
                record[key] = str(get_random_float())
            elif key in self._datetime_fields:
                # Random time in last year
                random_timestamp = int(time()) - get_random_int(0, 86400*365) # TODO read field range re dates
                record[key] = timestamp_to_utc(random_timestamp)
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

    def load_record_buffer(self, records):
        """ Adjust num messages to account for size of buffer. """
        bufferlen = len(self._buffered_records)
        self.__buffer_ind = 0
        self._no_msgs = bufferlen // self._recs_per_msg +1
        self._buffered_records = records

    def _get_buffered_record(self, *args):
        """ Allow records to be set externally """
        if self.__buffer_ind < len(self._buffered_records):
            next_record = self._buffered_records[self.__buffer_ind]
            self.__buffer_ind += 1
            return next_record
        else:
            return None

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

            if not record: # Handle end of buffered message
                return message

            JSON_MSG_DICT['UsageRecords'].append(record)

        return str(JSON_MSG_DICT).replace('"', "'")

    def get_apel_message_ordered(self, prefix, record_method='full'):
        """Get a valid message string, with its fields in the correct order."""
        message = self._header + "\n"
        for i in range(self._recs_per_msg):
            dict = self._record_methods[record_method](prefix + str(i))

            if not dict: # Handle end of buffered message
                return message

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

            if not record: # Handle end of buffered message
                return message

            record_ordered = {key: record[key] for key in self._all_fields if key in record}
            JSON_MSG_DICT['UsageRecords'].append(record_ordered)

        #return str(JSON_MSG_DICT).replace("'", '"')
        return json.dumps(JSON_MSG_DICT, indent=4).replace("'", '"')

    def get_apel_message_lowercase(self, prefix, record_method='full'):
        """Get a message with its key-value pairs in lower-case."""
        message = self._header + "\n"
        for i in range(self._recs_per_msg):
            dict = self._record_methods[record_method](prefix + str(i))

            if not dict: # Handle end of buffered message
                return message

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

        for i in range(self._recs_per_msg-1): # TODO does not write when cutting records-per-msg short
            dict = self._record_methods[record_method](prefix + str(i))

            if not dict: # Handle end of buffered message
                return message

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

            if not record: # Handle end of buffered message
                return message

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
            k = self._ordered_message_formats[fmt](prefix, record_method=method)
            f = open(filepath, 'w')
            f.write(k)
            f.close()

        print("Done.")


if __name__ == '__main__':
    print('Not implemented')