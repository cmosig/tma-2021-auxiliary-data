###  Auxiliary material for the paper "Revisiting Recommended BGP Route Flap Damping Configurations" 

Before you start using the scripts in this repository, please install our
branch of [libbgpstream](https://github.com/CAIDA/libbgpstream). 
To download and process BGP updates for a specific time period, you need to conduct the following steps:

1. Change into `data/` directory.
2. Update the epoch times in `config.ini`.
3. Run `../run_all.sh`.


If you want to reproduce our data, you should use the following configuration in `config.ini`:
```
[general]
prefixes = []
start-ts = 1590969600
end-ts = 1591574400
update-file-suffix = _week
input-file = 
```

**Warning:** Please be aware that our measurement period creates about 1TB data on your machine.

#### Overview of scripts

* `download_data.py`: Downloads BGP update dumps for the specified time period
  from the route collector projects RIPE RIS, RouteViews, and Isolario.
* `create_rc_mapping.sh`: Creates table consisting of `rc-project,
  rc-collector-name, peer-AS, peer-IP`.
* `split_dumps_fast.py`: Takes the output file of `download_data.py` and splits
  the dumps into multiple files (one file per vantage point) for parallel
  processing.
* `filter_duplicates.py`: Filters BGP duplicates.
* `track_penalty.py`: Simulates RFD for the given vendor and saves snapshots of
  prefix penalties at one minute intervals.
* `bgpana.py`: utility library

#### Contact

If you have any questions, please contact clemens.mosig@fu-berlin.de. 

