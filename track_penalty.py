import os
import numpy as np
import pandas as pd
import itertools
import configparser
import math
from tqdm import tqdm
import bgpana as bap
from collections import defaultdict
import gzip

# ------------------------------------------------------------
# TEST MODE
# ------------------------------------------------------------
test = False
if test:
    bap.log("YOU ARE IN TEST MODE")

# ------------------------------------------------------------
# Constants
# ------------------------------------------------------------
save_interval = 60  # seconds
# split_dir = "split_dumps_subset_for_rfd_simulation"
split_dir = "split_dump_raw" if not test else "test_dumps"

if not test:
    config = configparser.ConfigParser()
    config.read("config_week.ini")

do_done_check = False


def main(vendor, version):
    def process_vp(peer):
        # store last update type
        # init with empty string
        last_update_type = dict()

        # fill penalties dict
        # defaultdict with penalty inititally 0
        # prefix -> (penalty, last updated)
        penalties = dict()

        # set first second of the measurement
        last_ts = (int(config["general"]["start-ts"]) if not test else 0) - 1

        # open states file
        filename = f"{states_dir}/{peer['ip']}_{peer['rc']}_{version}_saved_states.gz"

        #if states file already exists, then quit
        if do_done_check and os.path.exists(filename):
            return
        else:
            saved_states = gzip.open(filename, "wb+")

        # get filename depending on IP version
        filename = f"{split_dir}/{peer['ip']}_{peer['rc']}_{version}_dumps_no_dupes.gz"

        # stop processing if file does not exist
        if not os.path.exists(filename):
            bap.log(f"file does not exist: {filename}")
            return

        for line in gzip.open(filename, "rb"):
            # parse update line
            try:
                message_type, upd_type, rc_project, rc_name, peer_AS, peer_ip,\
                        next_hop, path, origin_AS, communities, atomic_agg,\
                        agg_ip, agg_AS, med, isv6, prefix, ts \
                        = line.decode().split('|')
            except:
                bap.log(f"parsing error\n{line.decode()}\n{filename=}")

            # parse the timestamp as int because we process updates at second
            # granularity
            ts = int(float(ts.rstrip()))

            # if the lines are not sorted then there is an issue -> print
            if ts < last_ts:
                bap.log(
                    f"file is not sorted:{filename=}\n{line.decode()}{ts=}{last_ts=}"
                )

            # if we have reached a new second then ...
            if ts > last_ts:
                # find all save-timestamps between the new timestamp and the last timestamp
                # (not including the last timestamp, but including the new timestamp)
                # because of the mechanism, the first timestamp is not saved
                timestamp_to_save = [
                    save_time for save_time in range(last_ts + 1, ts + 1)
                    if save_time % save_interval == 0
                ]
                # save all states thare are to save if there are any
                for save_time in timestamp_to_save:
                    # update prefix penalties and save state
                    lines = ""
                    for prefix_, _ in penalties.items():
                        # calculate the difference from the last time the
                        # prefix was updated to the current save_time
                        assert penalties[prefix_][
                            "last_penalty_reduction"] != -1, "last penalty reduction cannot be -1"
                        delta = save_time - penalties[prefix_][
                            "last_penalty_reduction"]

                        assert delta >= 0, "delta can't be less than 0"

                        # determine the new penalty
                        # new penalty is N_0 * 0.5^(delta / half_life)
                        new_penalty = penalties[prefix_]["penalty"] * (0.5**(
                            delta / (half_life)))

                        # reset penalty if below half the reuse threshold
                        # this is what Cisco does
                        if new_penalty < reuse_threshold / 2:
                            new_penalty = 0

                        # store new penalty
                        penalties[prefix_]["penalty"] = new_penalty
                        # store time for which penalty has been calculated
                        penalties[prefix_][
                            "last_penalty_reduction"] = save_time

                        # line to save in states file
                        if new_penalty != 0:
                            lines += f"{save_time}|{peer['ip']}|{prefix_}|{new_penalty}\n"
                    saved_states.write(lines.encode())
                    del lines

            # update last_ts because saving has been done
            last_ts = ts

            # if first update for prefix then set correct values in dict
            if prefix not in penalties:
                last_update_type[prefix] = ""

                # set to the ts of the first update
                penalties[prefix] = {
                    "last_penalty_reduction": ts,
                    "penalty": 0
                }

            # if penalty has not been reduced by the save mechanism, then reduce it now
            if ts > penalties[prefix]["last_penalty_reduction"]:
                # calculate time delta to the last time we updated the penalty
                delta = ts - penalties[prefix]["last_penalty_reduction"]

                # update the penalty based on the time delta, but only for the
                # current prefix
                penalties[prefix]["penalty"] = penalties[prefix]["penalty"] * (
                    0.5**(delta / (half_life)))


                # reset penalty to 0 if below half the reuse-threshold
                # this is what ciso does according to their docs
                if penalties[prefix]["penalty"] < reuse_threshold / 2:
                    penalties[prefix]["penalty"] = 0

                # remember when you last updated the penalty
                penalties[prefix]["last_penalty_reduction"] = ts

            # increment penalty
            if upd_type == 'W':
                penalties[prefix]["penalty"] += withdrawal_penalty
            elif upd_type == 'A':
                if last_update_type[prefix] == 'A':
                    penalties[prefix]["penalty"] += attribute_change_penalty
                elif last_update_type[prefix] == 'W':
                    penalties[prefix]["penalty"] += readvertisement_penalty
                else:
                    # this happens only for the first update
                    # TODO does 500/1000 matter in this case?
                    penalties[prefix]["penalty"] += attribute_change_penalty

            # update last update type
            last_update_type[prefix] = upd_type

        # close states file
        saved_states.close()

    states_dir = f"states_all_{vendor}_{version}" if not test else "test_states"
    bap.prep_dir(states_dir)

    # ------------------------------------------------------------
    # RFD parameters
    # ------------------------------------------------------------
    withdrawal_penalty = 1000
    readvertisement_penalty = 1000 if vendor == "juniper" else 0
    attribute_change_penalty = 500
    half_life = 15 * 60  # seconds
    ignore_penalty_threshold = 750 / 2

    # IMPORTANT: MAX SUPPRESS TIME IS NOT IMPLEMENTED
    maximum_suppress_time = 60 * 60  # 60 minutes
    reuse_threshold = 750
    # https://tools.ietf.org/html/rfc2439
    # ceiling value formula in Section 4.5
    # max penalty = 12000
    maximum_penalty = reuse_threshold * (2
                                         **(maximum_suppress_time / half_life))

    # ------------------------------------------------------------
    # INIT
    # ------------------------------------------------------------

    if not test:
        peers = list(
            map(
                lambda line: dict(
                    zip(["project", "rc", "asn", "ip"], line.split('|'))),
                open(f"./rc_mapping_{version}").read().splitlines()))
        bap.paral(process_vp, [peers])
    else:
        process_vp({"rc": "test-rc", "ip": "test-ip"})


for vendor, version in itertools.product(["cisco", "juniper"], ["v4", "v6"]):
    bap.log(f"{version=}, {vendor=}")
    main(vendor, version)

    if test:
        break

if test:
    # check if output file is correct
    output_file = pd.read_csv("test_states/test-ip_test-rc_v4_saved_states.gz",
                              sep="|",
                              names=["ts", "ip", "prefix", "pen"],
                              dtype={
                                  "pen": np.float64
                              }).set_index(["ts", "prefix"])

    check_file = pd.read_csv("test_states/states_test_manual.gz",
                             sep="|",
                             names=["ts", "ip", "prefix", "pen"],
                             dtype={
                                 "pen": np.float64
                             }).set_index(["ts", "prefix"])

    pen_diff = check_file["pen"] - output_file["pen"]
    print(pen_diff)
    print(f"difference is small: {(pen_diff < 0.009).all()}")
