# filter_tstat_by_asn
Filter tstat tcp_complete.log entries by a given autonomous system number on a spark cluster.

## Setup 

Setup pyasn on your driver and workers.

https://pypi.org/project/pyasn/


## Input:

### download the ipasn file matching the dates on your tstat data

$ echo 20191008 > file_with_dates

$ pyasn_util_download.py --dates-from-file file_with_dates.txt 

$ pyasn_util_convert.py --single rib.youtfilename.bz2 ipasn_file

### choose yout tstat input and output files

hdfs:///your/hdfs/input_tstat_file/log_tcp_complete.gz

### edit run_filter_tstat_by_asn.sh

Target AS number, ipasn_file (--files and --local_asn_ipasn_file), tstat input, tstat output 

## run

./run_filter_tstat_by_asn.sh

