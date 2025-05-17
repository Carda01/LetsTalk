#!/bin/bash
export PYSPARK_PYTHON=/usr/local/bin/python
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python

exec python "$@"
