import dpkt
import socket
import pandas as pd
from pyspark.sql import SparkSession

with open('file.pcap', 'rb') as pcap_file:
    contents = dpkt.pcap.Reader(pcap_file)

    src_ip_ = []
    dst_ip_ = []
    s_port_ = []
    d_port_ = []
    bytes_cnt_ = []

    for (ts, buf) in contents:
        eth = dpkt.ethernet.Ethernet(buf)
        ip = eth.data
        try:
            src_ip = socket.inet_ntop(socket.AF_INET, ip.src)
            dst_ip = socket.inet_ntop(socket.AF_INET, ip.dst)
        except ValueError:
            src_ip = socket.inet_ntop(socket.AF_INET6, ip.src)
            dst_ip = socket.inet_ntop(socket.AF_INET6, ip.dst)

        tcp = ip.data
        s_port = tcp.sport
        d_port = tcp.dport

        bytes_cnt = len(buf)

        src_ip_.append(src_ip)
        dst_ip_.append(dst_ip)
        s_port_.append(s_port)
        d_port_.append(d_port)
        bytes_cnt_.append(bytes_cnt)

pandas_df = pd.DataFrame({
    'Source': src_ip_,
    'Destination': dst_ip_,
    'Source_Port': s_port_,
    'Destination_Port': d_port_,
    'Bytes': bytes_cnt_
})
spark = SparkSession.builder.getOrCreate()
spark_df = spark.createDataFrame(pandas_df)
spark_df.show()

pandas_df.to_parquet('df.parquet')

spark_df.groupby('Source').count().show()
spark_df.groupby('Source', 'Destination').count().show()
spark_df.groupby('Source', 'Destination').sum('Bytes').show()
