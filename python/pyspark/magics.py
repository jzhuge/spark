#!/usr/bin/env python
# -*- coding: utf-8 -*-

from IPython.core.magic import (register_line_magic, register_line_cell_magic)

from pyspark.context import SparkContext

@register_line_magic
def tail_log(arg = ""):
    try:
        num_lines = int(arg)
    except:
        num_lines = 100

    sc = SparkContext._active_spark_context
    if sc:
        log = open(sc._conf.get("spark.log.path"))
        lines = []

        line_count = 0
        for line in log.readlines():
            line_count += 1
            lines.append(line)
            # occasionally discard unnecessary lines
            if line_count > 10 * num_lines:
                lines = lines[-num_lines:]

        print "".join(lines[-num_lines:])

    else:
        print "Cannot load logs: no active SparkContext"
