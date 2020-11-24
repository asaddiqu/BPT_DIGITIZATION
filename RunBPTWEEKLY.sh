#!/bin/sh
declare -a years=('2016' '2017' '2018' '2019')
for year in  "${years[@]}"; do
  nohup python BPT_Digitization.py ${year} > /home/dsxusr/BPT_Digi/nohup_${year}.log &
done
