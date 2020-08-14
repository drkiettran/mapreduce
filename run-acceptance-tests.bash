#!/bin/bash

wc_output=$( yarn jar target/cisc-525-mapreduce-jar-with-dependencies.jar \
	com.drkiettran.mapreduce.WordCount \
	/user/student/shakespeare/tragedy/othello.txt \
	/tmp/othello )

fbc_output=$( yarn jar target/cisc-525-mapreduce-jar-with-dependencies.jar \
	com.drkiettran.mapreduce.FlightsByCarriers \
	/user/student/airline/1987.csv \
	/tmp/1987)

echo
echo

if [[ $wc_output == *"Total words"* ]]; then
  echo "WordCount runs successfully!"
else
  echo ${wc_output}
  echo "WordCount fails!"
fi


if [[ $fbc_output == *"Total flights"* ]]; then
  echo "FlightsByCarriers runs successfully!"
else
  echo ${fbc_output}
  echo "FlightsByCarriers fails!"
fi
