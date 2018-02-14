#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Must support both Python 2 and 3 !
#
from __future__ import print_function

import os, sys

from IPython.core.magic import (register_line_magic, register_line_cell_magic)

from pyspark.context import SparkContext

@register_line_magic
def tail_log(arg = ""):
    try:
        num_lines = int(arg)
    except:
        num_lines = 100

    log = open(os.environ['SPARK_LOG_FILE_PATH'])
    lines = []

    line_count = 0
    for line in log.readlines():
        line_count += 1
        lines.append(line)
        # occasionally discard unnecessary lines
        if line_count > 10 * num_lines:
            lines = lines[-num_lines:]

    print("".join(lines[-num_lines:]))
