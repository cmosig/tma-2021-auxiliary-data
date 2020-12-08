# creates table consisting of rc-project, rc-collector-name, peer-AS, peer-IP

filename=$1

# <dump-type>|<elem-type>|<record-ts>|<project>|<collector>|||<peer-ASn>|<peer-IP>|<prefix>|<next-hop-IP>|<AS-path>|<origin-AS>|<communities>|<old-state>|<new-state>|aggregator-ip

zcat $filename | cut -d '|' -f 4,5,8,9 | awk '!seen[$0]++' | sort > rc_mapping
