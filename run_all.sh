# WARNING this creates about 1TB data on your machine

python3 ../download_data.py config.ini
../create_rc_mapping.sh
python3 ../split_dumps_fast.py
python3 ../filter_duplicates.py
python3 ../track_penalty.py
