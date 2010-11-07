===============================
 Marionette Collective Plugins
===============================

This is a general dumping ground for my MCollective plugins.

connector/amqpcarrot
====================

MCollective supports STOMP out of the box.  I wanted it to support AMQP
natively, so I made a connector plugin that utilises the ``carrot`` library
(https://github.com/famoseagle/carrot).

The STOMP connector (thanks to its library) supports server failover, and is
recommended over this one at this stage.

This is also my first attempt at Ruby.  Yay me.

