Dashboard
*********

Rain Dashboard is implement in JavaScript and for the development and built, we
use NodeJs. However, we do not want to have Node.js as a hard depedancy when
Rain is built from sources. Therefore, compiled form Dashboard is included into
Rain git repository. Neverthless, if you want to work on Dashboard, you need to
Node.js.

Installation::

  cd dashboard
  npm install


Development
===========

For development, just run::

  npm start

It starts on Rain dashboard on port 3000. Now you can just edit dashboard
sources, **without** recompiling Rain binary. Dashboard in the development mode
assumes, that http rain server is running at localhost:8080. If you need, you
can change the address in ``dashboard/src/utils/fetch.js``, but do not commit
this change.


Deployment
==========

All Dashboard resources (including JS source codes) are included into Rain
binary. Rain compiles files in ``dashboard/dist`` into its binary. To generate
``dist`` directory from actual sources, you need to run::

  cd dashboard
  sh make_dist.sh

And then rebuild Rain (e.g. ``cargo build``). When you finish work on dashboard,
do not forget to include files in ``dist``.
