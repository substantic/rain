
Contributors's Guide
********************

We welcome contributions of any kind. Do not hesitate to open an GitHub issue or
contant us via email; this part of documentation is quite sparse.


Testing
=======

Rain contains two sets of tests:

  * Unittests (in Rust)
  * Integration tests (in Python)


Python tests
------------

Python tests are placed in `/rain/tests/pytests`.

To execute them simply run `py.test-3` in the root directory of Rain. The logs
are stored in `rain/tests/pytests/work`.

Important notes:

    * Make sure you are running Python 3 py.test.
    * Working directory `rain/tests/pytests/work` is fully cleaned before every
      test! Therefore, if you want to see logs, make sure that no other test is
      executed after the test you want to see. See options `-x` and `-k` of
      py.test-3
    * By default, Python tests run with rain binary located in
      `rain/target/debug/` directory. This path can be modified using
      RAIN_TEST_BIN environment variable.


Dashboard
=========

Rain Dashboard is implement in JavaScript over NodeJs. However, we do not want
to have Node.js as a hard dependency when Rain is built from sources. Therefore,
compiled form of Dashboard is included into Rain git repository. Neverthless, if
you want to work on Dashboard, you need to install Node.js.

Installation::

  cd dashboard
  npm install


Development
-----------

For development, just run::

  npm start

It starts on Rain dashboard on port 3000. Now you can just edit dashboard
sources, **without** recompiling Rain binary. Dashboard in the development mode
assumes, that http rain server is running at localhost:8080. If you need, you
can change the address in ``dashboard/src/utils/fetch.js``, but do not commit
this change.


Deployment
----------

All Dashboard resources (including JS source codes) are included into Rain
binary. Rain compiles files in ``dashboard/dist`` into its binary. To generate
``dist`` directory from actual sources, you need to run::

  cd dashboard
  sh make_dist.sh

And then rebuild Rain (e.g. ``cargo build``). When you finish work on dashboard,
do not forget to include files in ``dist``.
