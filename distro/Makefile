#
# Copyright (c) 2010 rPath, Inc.
#
# All Rights Reserved
#

.PHONY: all install

all: default-all

install: default-install
	install -D -m0644 repeaterrc $(DESTDIR)/etc/conary/rMN/repeaterrc
	install -D -m0755 repeater.init $(DESTDIR)/etc/init.d/repeater

include ../Make.rules
include ../Make.defs
# vim: set sts=8 sw=8 noexpandtab :