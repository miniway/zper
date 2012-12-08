#!/bin/bash
ps ax | grep -i 'zper.ZPer' | grep -v grep | awk '{print $1}' | xargs kill 
