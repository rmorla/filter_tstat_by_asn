# filter_tstat_by_asn
Given an autonomous system number, uses spark to filter the TCP flows of a tstat tcp_complete.log HDFS file by the AS number of the flow's server IP address.

## Setup 

Install pyasn on your spark driver and workers.

https://pypi.org/project/pyasn/


## Input

### download the ipasn file matching tstat dates, make it available on the driver.

$ echo 20191008 > file_with_dates

$ pyasn_util_download.py --dates-from-file file_with_dates.txt 

$ pyasn_util_convert.py --single rib.youtfilename.bz2 ipasn_file

### choose yout tstat input and output files

hdfs:///your/hdfs/input_tstat_file/log_tcp_complete.gz

hdfs:///your/hdfs/tstat_file_with_flows_from_given_asn

### edit run_filter_tstat_by_asn.sh

Choose: target AS number to filter with, ipasn_file (--files and --local_asn_ipasn_file), tstat input filename, tstat output filename

## run

./run_filter_tstat_by_asn.sh

