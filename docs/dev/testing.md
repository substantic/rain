Testing
=======

Python tests
------------

Python tests are placed in rain/tests/pytests

To execute them simply run `py.test-3` in the root directory of Rain. The logs
are stored in ``rain/tests/pytests/work`

Important notes:
* Run `py.test-3` not `py.test` (for Python 2)
* Working directory `rain/tests/pytests/work` are fully cleaned before every test!
  So if you want to see logs, make sure that no other test is executed after
  the test you want to see. See options `-x` and `-k` of py.test-3

