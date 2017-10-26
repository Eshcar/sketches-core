#!/bin/bash


                for writers in 1 2 4 8 12 16 20 24 28;
                do
                                for i in {1..3}; do
                                        if ((i == 1)); then
                                                 numactl -l java com.yahoo.sketches.theta.TestPerformnceTheta LOCK_BASED_ORIGENAL $writers  30 true
                                        else
                                                 numactl -l java com.yahoo.sketches.theta.TestPerformnceTheta LOCK_BASED_ORIGENAL  $writers  30 false
                                        fi
                                 done


                done
exit 0
