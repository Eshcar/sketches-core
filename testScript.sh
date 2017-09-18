#!/bin/bash


        for levels in {0..6};
        do
                for writers in {1..8};
                do
                                for i in {1..5}; do
                                        if ((i == 1)); then
                                                 numactl -N 0 -m 0 java  com.yahoo.sketches.quantiles.TestPerformance $writers $levels  30 true
                                        else
                                                 numactl -N 0 -m 0 java  com.yahoo.sketches.quantiles.TestPerformance $writers $levels  30 false
                                        fi
                                 done


                done
        done
exit 0
