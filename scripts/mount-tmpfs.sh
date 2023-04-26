#!/bin/bash
TMPFSDIR=/tmp/parsec_sweep
mkdir -p ${TMPFSDIR}
if ! mountpoint ${TMPFSDIR} > /dev/null ; then
  echo "Mounting ${TMPFSDIR}"
  mount -t tmpfs -o mpol=bind:0,huge=always,size=10% tmpfs ${TMPFSDIR}
fi
