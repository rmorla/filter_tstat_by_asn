import sys
import argparse
import pyasn
from pyspark.sql import SparkSession

def f_parse_options():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target_asn", default="0")
    parser.add_argument("--local_asn_ipasn_file", default="/dev/null")
    parser.add_argument("--hdfs_tstat_input_file", default="/dev/null")
    parser.add_argument("--hdfs_tstat_output_file", default="/dev/null")

    (opts, args) = parser.parse_known_args(sys.argv)
    return (opts, args)

def f_init():
    (opts, args) = f_parse_options()
    spark = SparkSession.builder.appName("filter_tstat_by_asn").getOrCreate()
    return (opts, spark)

# worker-level global for one-off loading of ipasn file
worker_asndb = None

def filter_tstat_by_asn_worker_fn(tstat_row, target_asn, local_asn_ipasn_file):

    # one off loading of ipasn file in each worker, ipasn file copied to worker with spark-submit --files
    global worker_asndb
    if worker_asndb == None:
        worker_asndb = pyasn.pyasn(local_asn_ipasn_file)

    server_ip = tstat_row['s_ip:15']

    # asndb.lookup('8.8.8.8')  -> (15169, 8.8.8.0/24)
    asn = worker_asndb.lookup(server_ip)
    return str(asn[0]) == str(target_asn)

def filter_tstat_by_asn(spark, target_asn, local_asn_ipasn_file, hdfs_tstat_input_file, hdfs_tstat_output_file):

    df = spark.read.csv(hdfs_tstat_input_file, header=True, sep=' ')

    # cannot filter directly on dataframe, using rdd with [Row] objects
    rdd_filtered = df.rdd.filter(lambda x: filter_tstat_by_asn_worker_fn(x, target_asn, local_asn_ipasn_file))

    # create a dataframe with filtered rdd and original tstat header
    df_filtered = spark.createDataFrame(rdd_filtered, schema=df.schema)
    df_filtered.write.csv(hdfs_tstat_output_file, header=True, sep=' ')


def main():
    (opts, spark) = f_init()
    filter_tstat_by_asn(spark, opts.target_asn, opts.local_asn_ipasn_file, opts.hdfs_tstat_input_file, opts.hdfs_tstat_output_file)


if __name__ == '__main__':
    main()
