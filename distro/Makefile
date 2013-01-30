#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
