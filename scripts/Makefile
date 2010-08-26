#
# Copyright (c) 2005-2010 rPath, Inc.
#
# All Rights Reserved
#

static_files =				\
	management_node

override STATICDEST = /usr/share/rbuilder/scripts

all: default-all

install: default-install
	install -d -m0755 $(DESTDIR)/$(STATICDEST)
	install -m0755 $(static_files)  $(DESTDIR)/$(STATICDEST)

clean: default-clean


include ../Make.rules
# vim: set sts=8 sw=8 noexpandtab :
