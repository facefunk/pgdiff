#!/bin/bash

passed=0
failed=0
for f in test-*; do
    got=$(./"$f")
    need=$(<"output/$f")
    dif=$(diff -uw <(echo "$need") <(echo "$got"))
    if [ "$dif" != "" ]; then
        failed=$((failed+1))
        echo "$f failed:"
        echo "$dif"
        echo
    else
        passed=$((passed+1))
    fi
done

test "$passed" -gt 0 && echo "$passed tests passed."
test "$failed" -gt 0 && echo "$failed tests failed."
