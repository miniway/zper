#!/bin/bash

nohup nice -n 0 $(dirname $0)/zper-run-class.sh org.zeromq.zper.ZPer $@ &
