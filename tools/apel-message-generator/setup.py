#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='apel_message_generator',
    author='Adrian Coveney/Connor Pettitt',
    description='Generate messages for the APEL system.',
    version='0.1',
    packages=find_packages(),
    entry_points = {
        'console_scripts': [
            'apel_generate_messages = apel_message_generator.gen_records:main',
            'apel_generate_gpu = apel_message_generator.generators.gpu:main',
            'apel_generate_gpusummary = apel_message_generator.generators.gpu:main_summary',
            'apel_generate_join_job = apel_message_generator.generators.join_job:main',
            'apel_generate_job = apel_message_generator.generators.job:main',
            'apel_generate_summary = apel_message_generator.generators.summary:main',
            'apel_generate_event = apel_message_generator.generators.event:main',
            'apel_generate_blahd = apel_message_generator.generators.blahd:main',
            'apel_generate_spec = apel_message_generator.generators.spec:main',
        ],
    }
    )
