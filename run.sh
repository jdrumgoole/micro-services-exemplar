#!/bin/sh
prog=$1
shift
python $prog --host mongodb://localhost:27017/?replicaset=ecom $*
