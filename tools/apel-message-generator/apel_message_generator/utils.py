import random
import sys
import datetime

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

def utc_to_timestamp(utc):
    return datetime.datetime.strptime(utc, '%Y-%m-%d %H:%M:%S').timestamp()

def timestamp_to_utc(timestamp):
    return datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


def usage():
    """Print a usage message."""
    print("Usage: " + sys.argv[0] + \
        """ [-r <recs-per-msg> -m <no-msgs> -d <directory> -f <format>] jobs|summaries|gpu

         Defaults: recs-per-msg: 1000
                   no-msgs:      100
                   directory:    ./job-msgs | ./sum-msgs
                   format:       apel (csv)
        """)
