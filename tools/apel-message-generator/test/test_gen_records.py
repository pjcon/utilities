#!/bin/env python2.7

import unittest

from gen_records import (
    JobRecordGenerator,
    SummaryRecordGenerator,
    GPURecordGenerator,
    GPUSummaryGenerator,
    BlahdRecordGenerator,
    EventRecordGenerator,
    SpecRecordGenerator,
    JoinJobRecordsGenerator
)

jrg = JobRecordGenerator(recs_per_msg, no_msgs, msg_dir)
jrg.write_messages(msg_fmt)

srg = SummaryRecordGenerator(recs_per_msg, no_msgs, msg_dir)
srg.write_messages(msg_fmt)

grg = GPURecordGenerator(recs_per_msg, no_msgs, msg_dir)
grg.write_messages(msg_fmt)

gsg = GPUSummaryGenerator(recs_per_msg, no_msgs, msg_dir)
gsg.write_messages(msg_fmt)

brg = BlahdRecordGenerator(recs_per_msg, no_msgs, msg_dir)
brg.write_messages(msg_fmt)

erg = EventRecordGenerator(recs_per_msg, no_msgs, msg_dir)
erg.write_messages(msg_fmt)

srg = SpecRecordGenerator(recs_per_msg, no_msgs, msg_dir)
srg.write_messages(msg_fmt)

jjrg = JoinJobRecordsGenerator(recs_per_msg, no_msgs, msg_dir)
jjrg.write_messages(msg_fmt)


""" Cases
- format: apel, json, csv, bob
- number of messages: -1, 0, 1, 10000
- number of records per message: -1, 0, 1, 10000

Check that:
- generates messages at all
- messages can be loaded by apel.load_message (or similar proxy)
"""

class JobRecordGeneratorTest(unittest.TestCase):
    '''
    Test case for 
    '''

    def setUp(self):
        # Create test dir
        pass

    def test_x(self):
        pass

    def tearDown(self):
        # Remove test dir
        pass

class SummaryRecordGeneratorTest(unittest.TestCase):
    '''
    Test case for 
    '''

    def setUp(self):
        pass

    def test_x(self):
        pass

class GPURecordGeneratorTest(unittest.TestCase):
    '''
    Test case for GPURecord
    '''

    def setUp(self):
        pass

    def test_x(self):
        pass

class GPUSummaryGeneratorTest(unittest.TestCase):
    '''
    Test case for 
    '''

    def setUp(self):
        pass

    def test_x(self):
        pass

class EventRecordGeneratorTest(unittest.TestCase):
    '''
    Test case for 
    '''

    def setUp(self):
        pass

    def test_x(self):
        pass

class BlahdRecordGeneratorTest(unittest.TestCase):
    '''
    Test case for 
    '''

    def setUp(self):
        pass

    def test_x(self):
        pass

class SpecRecordGeneratorTest(unittest.TestCase):
    '''
    Test case for 
    '''

    def setUp(self):
        pass

    def test_x(self):
        pass

class JoinJobRecordsGeneratorTest(unittest.TestCase):
    '''
    Test case for 
    '''

    def setUp(self):
        pass

    def test_x(self):
        pass


if __name__ == '__main__':
    unittest.main()



