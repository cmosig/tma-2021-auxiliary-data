import fileinput
import bgpana as bap
import pandas as pd
import sys
import configparser
import gzip
import os
import time

# ------------------------------------------------------------
# Note that this does not work if the route collector column
# is filled with NAs
# ------------------------------------------------------------

# ------------------------------------------------------------
# Prep Dirs
# ------------------------------------------------------------
config = configparser.ConfigParser()
config.read("config_week.ini")

# where to save files
split_dir = "split_dump_raw"

# complete update dump
input_file = config["general"]["input-file"]

# create split dir if it does not exist
bap.prep_dir(split_dir)

# <dump-type>|<elem-type>|<record-ts>|<project>|<collector>|||<peer-ASn>|<peer-IP>|<prefix>|<next-hop-IP>|<AS-path>|<origin-AS>|<communities>|<old-state>|<new-state>|atomic-agg|agg-ip|agg-asn|med


def print_to_file(chunk):
    group_id, df_group = chunk
    peer_ip, rc, version = group_id
    if version:
        filename = f"{split_dir}/{peer_ip}_{rc}_v6_dumps.gz"
    else:
        filename = f"{split_dir}/{peer_ip}_{rc}_v4_dumps.gz"
    file_handle = gzip.open(filename, "a+")
    file_handle.write(
        df_group.to_csv(sep='|', header=None, index=False).encode())


def main():
    # for the 2020 dataset the below chunksize was fastest
    # chunksize = 5 * 10**7
    chunksize = 10**7
    lines_processed = 0
    for df_chunk in pd.read_csv(
            input_file,
            sep='|',
            header=None,
            names=[
                "message-type", "upd-type", "ts", "rc-project", "rc-name",
                "router-name", "router-ip", "peer-AS", "peer-ip", "prefix",
                "next-hop", "path", "origin-AS", "communities", "old-state",
                "new-state", "atomic-agg", "agg-ip", "agg-AS", "med"
            ],
            usecols=[
                "message-type", "upd-type", "ts", "rc-project", "rc-name",
                "peer-AS", "peer-ip", "prefix", "next-hop", "path",
                "origin-AS", "communities", "atomic-agg", "agg-ip", "agg-AS",
                "med"
            ],
            dtype=str,
            chunksize=chunksize):

        # filter out only bgp udpates (no state messages)
        df_chunk = df_chunk[df_chunk["message-type"] == 'U']

        # figure out ip version
        df_chunk["version"] = df_chunk["prefix"].str.contains(':')

        # group by peer, route collector, and IP version
        df_chunk_groups = df_chunk.groupby(["peer-ip", "rc-name", "version"])

        bap.paral(print_to_file, [df_chunk_groups])
        # for group_id, df_group in df_chunk_groups:

        lines_processed += chunksize
        bap.log(f"lines processed: {lines_processed}")


main()
