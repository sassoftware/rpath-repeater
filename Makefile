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

all: all-subdirs default-all

all-subdirs:
	for d in $(MAKEALLSUBDIRS); do make -C $$d DIR=$$d || exit 1; done

export TOPDIR = $(shell pwd)
export TIMESTAMP = $(shell python -c "import time; print time.time(); exit;")
export CFGDEVEL=repeaterrc

SUBDIRS=rpath_repeater rmake_plugins distro scripts
MAKEALLSUBDIRS=rpath_repeater rmake_plugins distro scripts

extra_files = \
	Make.rules              \
	Makefile                \
	Make.defs               \
	NEWS                    \
	README                  \
	LICENSE


.PHONY: clean dist install subdirs html

subdirs: default-subdirs

install: install-subdirs

clean: clean-subdirs default-clean

doc: html

html:   
	ln -fs plugins/ rbuild_plugins
	scripts/generate_docs.sh
	rm -f rbuild_plugins

dist:
	if ! grep "^Changes in $(VERSION)" NEWS > /dev/null 2>&1; then \
	        echo "no NEWS entry"; \
	        exit 1; \
	fi
	$(MAKE) forcedist


archive:
	hg archive  --exclude .hgignore -t tbz2 rpath-repeater-$(VERSION).tar.bz2

forcedist: archive

forcetag:
	hg tag -f rpath-repeater-$(VERSION)

tag:
	hg tag rpath-repeater-$(VERSION)

devel:

        
clean-devel:


clean: clean-devel clean-subdirs default-clean 

include Make.rules
include Make.defs
 
# vim: set sts=8 sw=8 noexpandtab :
