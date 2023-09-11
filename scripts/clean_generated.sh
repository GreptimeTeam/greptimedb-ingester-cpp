#!/usr/bin/bash

if [ -d proto/greptime ]; then
		rm -rf proto/greptime
fi

if [ -d proto/prometheus ]; then
		rm -rf proto/prometheus
fi

if [ -d proto/substrait_extension ]; then
		rm -rf proto/substrait_extension
fi
