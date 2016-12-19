#!/bin/bash
iostat | sed 1,6d | awk '{sum3+=$3; sum4+=$4; sum5+=$5; sum6+=$6} END {print sum5,sum6}'
