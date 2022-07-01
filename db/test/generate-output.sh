#!/bin/bash

for f in test-*
do
    echo "$f"
    "./$f" > "output/$f"
done
