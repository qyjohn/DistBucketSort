#!/bin/bash
iostat | sed 1,6d | awk '{sum5+=$5; sum6+=$6} END {print sum5,sum6}'
