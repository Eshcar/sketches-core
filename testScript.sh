#!/bin/bash



                for writers in {1..8};
                do
                                for i in {1..5}; do
					if ((i == 1)); then		
                                        	 numactl -N 0 -m 0 java  com.yahoo.sketches.quantiles.TestPerformance $writers 30 true
					else
						 numactl -N 0 -m 0 java  com.yahoo.sketches.quantiles.TestPerformance $writers 30 false
                               		fi	
				 done


                done
exit 0
