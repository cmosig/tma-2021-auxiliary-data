from collections import defaultdict
from datetime import datetime as dt
from tqdm import tqdm
import bgpana as bap
import configparser
import itertools
import os
import os.path
import shutil
import subprocess
import sys

url_suffixes = None
remove_route_collector_merge_files = True
merge_route_collector_files = True


def get_url_suffixes(start_ts, end_ts):
    url_suffixes = []  #[(month,url_suffix)]

    # find first and last 5 minute mark
    # we download files 15 min before the actual start so that we do not miss any
    # updates other files with possibly incorrect timestamps
    first_5_min_mark = ((((start_ts - 1) // 300) + 1) * 300) - 900
    last_5_min_mark = (end_ts // 300) * 300
    all_5_min_marks = [
        first_5_min_mark + 300 * i
        for i in range((last_5_min_mark - first_5_min_mark) // 300 + 1)
    ]

    #create url strings from timestamps
    #format: (yyyy_mm,yyyymmdd.HHMM.bz2)
    url_suffixes = [(dt.utcfromtimestamp(ts).strftime('%Y_%m'),
                     dt.utcfromtimestamp(ts).strftime('%Y%m%d.%H%M') + '.bz2')
                    for ts in all_5_min_marks]
    return url_suffixes


def exec_command(command):
    subprocess.Popen(command, shell=True).wait()


def _download_dumps(rc_project, rc_names):
    temporary_work_directory = f'.temp_download_{rc_project}.dump'
    if os.path.exists(temporary_work_directory):
        print("Caution! Temp dir already exists")
        return
    else:
        os.mkdir(temporary_work_directory)

    bap.log(f"{rc_project}: downloading files")
    global url_suffixes
    commands = []
    created_files = []
    for rc in rc_names:
        for url_suffix in url_suffixes:
            # depending on the route rc-project we need a different URL format
            if rc_project == "routeviews":
                if rc == "route-views2":
                    rc_string = ""
                else:
                    rc_string = f"/{rc}"
                url = f"http://archive.routeviews.org{rc_string}/bgpdata/{url_suffix[0].replace('_', '.')}/UPDATES/updates.{url_suffix[1]}"

            elif rc_project == "isolario":
                url = f"https://www.isolario.it/Isolario_MRT_data/{rc}/{url_suffix[0]}/updates.{url_suffix[1]}"

            elif rc_project == "ris":
                url = f"http://data.ris.ripe.net/{rc}/{url_suffix[0].replace('_', '.')}/updates.{url_suffix[1].replace('bz2', 'gz')}"

            # store files for later use here
            temp_file_name = f"{temporary_work_directory}/{rc}_{url_suffix[1]}".replace(
                "bz2", "").strip()

            bgpreader_command = f"bgpreader {' '.join(bgpreader_arguments)} -d singlefile -o upd-file={url} 2> /dev/null | awk -F '|' '" + "{" + f"OFS = FS; $4=\"{rc_project}\"; $5=\"{rc}\"; print;" + "}'" + f"| gzip > {temp_file_name}"

            commands.append(bgpreader_command)
            created_files.append(temp_file_name)

    bap.paral(exec_command, [commands])

    # merge files
    # replace this part of the code if you don't want one huge file
    bap.log(f"{rc_project}: merging files...")
    output_filename = f"{rc_project}_{start_ts}_{end_ts}.dump.gz"
    for file_ in created_files:
        merge_command = f"cat {file_} >> {output_filename}"
        subprocess.Popen(merge_command, shell=True).wait()
    shutil.rmtree(temporary_work_directory)

    return output_filename


def download_routeviews():
    route_collectors = [
        "route-views2", "route-views.sg", "route-views.perth",
        "route-views.sfmix", "route-views.mwix", "route-views.rio",
        "route-views.fortaleza", "route-views.gixa", "route-views3",
        "route-views4", "route-views6", "route-views.amsix",
        "route-views.chicago", "route-views.chile", "route-views.eqix",
        "route-views.flix", "route-views.gorex", "route-views.isc",
        "route-views.kixp", "route-views.jinx", "route-views.linx",
        "route-views.napafrica", "route-views.nwax", "route-views.phoix",
        "route-views.telxatl", "route-views.wide", "route-views.sydney",
        "route-views.saopaulo", "route-views2.saopaulo", "route-views.soxrs"
    ]

    return _download_dumps(rc_project="routeviews", rc_names=route_collectors)


def download_isolario():
    route_collectors = ["Alderaan", "Dagobah", "Korriban", "Naboo", "Taris"]
    return _download_dumps(rc_project="isolario", rc_names=route_collectors)


def download_ripe_ris():
    route_collectors = [
        "rrc00", "rrc01", "rrc03", "rrc04", "rrc05", "rrc06", "rrc07", "rrc10",
        "rrc11", "rrc12", "rrc13", "rrc14", "rrc15", "rrc16", "rrc18", "rrc19",
        "rrc20", "rrc21", "rrc22", "rrc23", "rrc24"
    ]
    return _download_dumps(rc_project="ris", rc_names=route_collectors)


def download_updates(configfile):
    config = configparser.ConfigParser()
    config.read(configfile)
    global start_ts
    start_ts = config["general"]["start-ts"]
    global end_ts
    end_ts = config["general"]["end-ts"]
    prefixes = eval(config["general"]["prefixes"])

    # generate suffixes from timestamps
    global url_suffixes
    url_suffixes = get_url_suffixes(int(start_ts), int(end_ts))

    global bgpreader_arguments
    bgpreader_arguments = []
    # this is important because we download more files than we need
    bgpreader_arguments.append('-w ' + str(start_ts) + ',' + str(end_ts))
    bgpreader_arguments.append('-t updates')
    for prefix in prefixes:
        bgpreader_arguments.append('-k ' + prefix)

    # file suffix to be used instead of all filtered prefixes
    file_suffix = ''
    if (config["general"]["update-file-suffix"] != ""):
        file_suffix = '_' + config["general"]["update-file-suffix"]
    else:
        if (len(prefixes) > 0):
            file_suffix = ('_' + '_'.join(prefixes)).replace('/', '_')

    filename = 'updates_' + start_ts + '_' + end_ts + file_suffix + ".dump.gz"

    # setting filename in config
    config["general"]["input-file"] = filename
    with open(configfile, 'w') as f:
        config.write(f)

    # download and filter data
    isolario_output_file = download_isolario()
    riperis_output_file = download_ripe_ris()
    routeviews_output_file = download_routeviews()

    # merge all route collectore files
    if merge_route_collector_files:
        merge_command = f"cat\
                {isolario_output_file}\
                {routeviews_output_file}\
                {riperis_output_file}\
                > {filename}"

        subprocess.Popen(merge_command, shell=True).wait()

    # remove other files
    if remove_route_collector_merge_files:
        os.remove(isolario_output_file)
        os.remove(routeviews_output_file)
        os.remove(riperis_output_file)


if (__name__ == "__main__"):
    download_updates("config.ini")
