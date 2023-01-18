#!/bin/bash
# usage: ${0} ...*.perf.data
parallel "perf stat report -i {1} &> {1}.txt" ::: "$@"
