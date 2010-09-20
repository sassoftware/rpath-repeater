#
# Copyright (c) 2010 rPath, Inc.
#
# All Rights Reserved
#

.PHONY: all install

all: default-all

install: default-install
	install -D -m0644 disp.cfg $(DESTDIR)/etc/rmake3/server.d/25_repeater.conf
	install -D -m0644 worker.cfg $(DESTDIR)/etc/rmake3/node.d/25_repeater.conf
	echo zone Local rBuilder >$(DESTDIR)/etc/rmake3/node.d/50_zone.conf

include ../Make.rules
include ../Make.defs
# vim: set sts=8 sw=8 noexpandtab :
