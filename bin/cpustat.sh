#!/bin/bash
# %usr, %sys, %iowait
mpstat 1 1 | sed 1,4d | awk '{print $3,$5,$6}'
