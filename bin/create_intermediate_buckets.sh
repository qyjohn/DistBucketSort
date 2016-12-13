#!/bin/bash
for i in {0..9}
do
    cd /disk000$i/intermediate
    rm -Rf *
    mkdir bucket{0000..0999}
done
