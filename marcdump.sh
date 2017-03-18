#!/bin/sh
# -*- coding: utf-8 -*-

SORT_OPTS="--ignore-case"
export LC_COLLATE="ca_ES.UTF-8"

what=marcdump
dbname=$LOGNAME.db
filename=registres.txt

datadir=~/var/lib/marcdump

amd=$(date +"a%Ym%md%d" | sed 's|m0|m|; s|d0|d|')
today=$(date +"%Y.%m.%d (%a %d %b %Y)")
weekday=$(date +"%u")

cd $datadir

~/bin/$what.py -u $dbname
~/bin/$what.py -d $dbname >$filename

awk '$1 ~ /^[167]00$/ && ! /\$[gt]/ { print substr($0, 11) }
    ' $filename | sort $SORT_OPTS | uniq -c | awk '{ print substr($0, 3) }
    ' >persones.txt
awk '$1 ~ /^[167]10$/ && ! /\$[gt]/ { print substr($0, 11) }
    ' $filename | sort $SORT_OPTS | uniq -c | awk '{ print substr($0, 3) }
    ' >institucions.txt
awk '$1 ~ /^[167]11$/ && ! /\$[gt]/ { print substr($0, 11) }
    ' $filename | sort $SORT_OPTS | uniq -c | awk '{ print substr($0, 3) }
    ' >congressos.txt
awk '$1 ~ /^650$/ && ! /\$[gt]/ { print substr($0, 11) }
    ' $filename | sort $SORT_OPTS | uniq -c | awk '{ print substr($0, 3) }
    ' >materies.txt
awk '$1 ~ /^655$/ && ! /\$[gt]/ { print substr($0, 11) }
    ' $filename | sort $SORT_OPTS | uniq -c | awk '{ print substr($0, 3) }
    ' >genere-forma.txt
awk '$1 ~ /^651$/ { print substr($0, 11) }
    ' $filename | sort $SORT_OPTS | uniq -c | awk '{ print substr($0, 3) }
    ' >geografics.txt

test -d .git || git init
git status
git add *.txt
git commit --all --message="$today updates"
for filename in *.txt; do
    file=$(basename $filename .txt)
    git show $filename >${file}.log
done

if [ $weekday = 5 ]; then
    git gc
    for filename in *.txt; do
	file=$(basename $filename .txt)
	git blame $filename | awk '{ print substr($0, 50, 10), substr($0, 84) }' >${file}.hist &
	git diff @{"1 week ago"} -p $filename >${file}_${amd}.log
	while [ $(ls -v ${file}_*.log | wc -w) -gt 6 ]; do
	    rm -v $(ls -v ${file}_*.log | head -1)
	done
    done
fi
