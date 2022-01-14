#!/bin/env python3
"""Module for generating test messages for the new APEL system.

It will currently generate job messages or summary messages, depending on
command-line arguments, but shouldn't be difficult to extend to other types of
message.  Create a subclass of RecordGenerator for each message type.
"""

import getopt
import sys

from apel_message_generator.generators import (
    job, summary, spec, blahd, event, join_job, gpu
)

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
        "jobs", "summaries", "gpu", "event", "blahd", "spec", "join-job-records", "gpusummary"
    ]

    if "jobs" in args:
        jrg = job.JobRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        jrg.write_messages(msg_fmt)
    elif "summaries" in args:
        srg = summary.SummaryRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        srg.write_messages(msg_fmt)
    elif "gpu" in args:
        grg = gpu.GPURecordGenerator(recs_per_msg, no_msgs, msg_dir)
        grg.write_messages(msg_fmt)
    elif "gpusummary" in args:
        gsg = gpu.GPUSummaryGenerator(recs_per_msg, no_msgs, msg_dir)
        gsg.write_messages(msg_fmt)
    elif "blahd" in args:
        brg = blahd.BlahdRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        brg.write_messages(msg_fmt)
    elif "event" in args:
        erg = event.EventRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        erg.write_messages(msg_fmt)
    elif "spec" in args:
        srg = spec.SpecRecordGenerator(recs_per_msg, no_msgs, msg_dir)
        srg.write_messages(msg_fmt)
    elif "join-job-records" in args:
        jjrg = join_job.JoinJobRecordsGenerator(recs_per_msg, no_msgs, msg_dir)
        jjrg.write_messages(msg_fmt)
    else:
        print(f"Argument must be one of: {', '.join(allowed_args)}.")
        usage()
        sys.exit()

