#!/bin/bash



if ((2>1)); then
	echo "bo"
fi

for helpers in {1..10};
do
	for levels in {1..6};
	do

		for writers in 1 2 4 8 16 32;
		do
			lev=$((levels - 1))	
			leaves=$((2**lev))
			if ((leaves >= writers)); then	
				echo "############# helpers = $helpers  levels = $levels  writers = $writers  ##################################################################################"
				for i in {1..5}; do
				#	echo "test"
					java  com.yahoo.sketches.quantiles.TestPerformance $levels $helpers $writers 1
				done
			fi

		
		done

	done 

done
exit 0
