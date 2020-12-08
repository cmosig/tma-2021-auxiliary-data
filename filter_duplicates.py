import pandas as pd
import time
import bgpana as bap
import os

# ------------------------------------------------------------
# this script is supposed to filter out duplicates per prefix 
# WARNING: uses a lot of RAM, adjust num_cores according to your setup
# ------------------------------------------------------------

do_done_check = False


def filter_duplicates(filename):
    global duplicate_absolutes
    bap.log(f"processing:\t {filename}")

    # check if already done
    save_filename = f"{filename.replace('.gz', '_no_dupes.gz')}"
    if do_done_check:
        if os.path.isfile(save_filename):
            bap.log(f"file exists:\t {filename}")
            return

    try:
        df = pd.read_csv(
            filename,
            sep='|',
            header=None,
            names=[
                "message-type", "upd-type", "ts", "rc-project", "rc-name",
                "peer-AS", "peer-ip", "prefix", "next-hop", "path",
                "origin-AS", "communities", "atomic-agg", "agg-ip", "agg-AS",
                "med", "isv6"
            ],
            dtype={
                "message-type": "category",
                "upd-type": "category",
                "ts": "float",
                "rc-project": "category",
                "rc-name": "category",
                "peer-AS": "category",
                "peer-ip": "category",
                "prefix": "string",
                "next-hop": "category",
                "path": "string",
                "origin-AS": "string",
                "communities": "string",
                "atomic-agg": "category",
                "agg-ip": "string",
                "agg-AS": "string",
                "med": "category",
                "isv6": "category"
            },
        )
        # nrows=100000)
    except:
        # for empty files
        bap.log(f"empty file:\t {filename}")
        return (0, 0)

    # save original update count for later
    original_size = df.shape[0]

    # fill all na values with an empty string, because None does not equal None
    for column_name in df.columns.tolist():
        if df[column_name].dtype.name == "category":
            df[column_name] = df[column_name].cat.add_categories([""
                                                                  ]).fillna("")

    # remove duplicates
    df["original_order"] = pd.Series(range(0, df.shape[0]))
    df = df.sort_values(by=["prefix", "original_order"])
    compare_columns = [
        "message-type", "upd-type", "rc-project", "rc-name", "peer-AS",
        "peer-ip", "next-hop", "path", "origin-AS", "communities",
        "atomic-agg", "agg-ip", "agg-AS", "med", "isv6", "prefix"
    ]
    df = df[~(df[compare_columns] == df[compare_columns].shift(1)).all(axis=1)]
    df = df.set_index("original_order")
    df = df.sort_index()

    # take only necessary columns
    df = df[[
        "message-type", "upd-type", "rc-project", "rc-name", "peer-AS",
        "peer-ip", "next-hop", "path", "origin-AS", "communities",
        "atomic-agg", "agg-ip", "agg-AS", "med", "isv6", "prefix", "ts"
    ]]

    # also save how many duplicates there were
    without_dupes_size = df.shape[0]
    dupes_count = (original_size, without_dupes_size)

    # clean up agg-AS
    df["agg-AS"] = df["agg-AS"].str.strip()

    # save no_duplicates_file
    df.to_csv(save_filename,
              sep='|',
              header=None,
              index=False,
              compression="gzip")
    bap.log(f"done:\t {save_filename}")

    return dupes_count


dirname = "split_dump_raw"
filenames = [
    f"{dirname}/{name}" for name in os.listdir(dirname) if "dupe" not in name
]
dupe_res = bap.paral(filter_duplicates, [filenames], num_cores=10)
# saves portion of duplicates
pd.Series(dict(zip(filenames, dupe_res))).to_csv("duplicate_absolutes.csv",
                                                 sep='|',
                                                 header=None)
