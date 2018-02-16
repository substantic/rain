Testing
=======

Python tests
------------

Python tests are placed in `/rain/tests/pytests`.

To execute them simply run `py.test-3` in the root directory of Rain. The logs
are stored in `rain/tests/pytests/work`

Important notes:
* Run `py.test-3` not `py.test` (for Python 2)
* Working directory `rain/tests/pytests/work` is fully cleaned before every test!
  Therefore, if you want to see logs, make sure that no other test is executed after
  the test you want to see. See options `-x` and `-k` of py.test-3
* By default, Python tests run with rain binary located in `rain/target/debug/` directory.
  This path can be modified using RAIN_TEST_BIN environment variable.
  
