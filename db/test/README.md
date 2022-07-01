These tests are now somewhat automated. By running run-tests.sh each script with the filename pattern test-* will be run
and its output compared against the corresponding stored output in ./output; if the output differs, except by length of
whitespace, the test will be said to have failed.

Connect to the database manually:
  sudo su - postgres -- -c "psql -d db1"

