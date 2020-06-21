#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The main script for scraping the FIRDS XML data.  Contains functions
for iterating through each week in a reference time period, fetching the
relevant XML files, analysing the contents and building a CSV file with
aggregated data about the benchmark rates.
"""

import multiprocessing as mp
from multiprocessing.managers import ListProxy
from copy import deepcopy
from typing import Optional, Tuple, Iterator
from datetime import datetime, timedelta

import fetch_data
import analyse_data

from benchmark_data import get_libors, get_non_libors

START_DATE = datetime(2018, 1, 1)
END_DATE = datetime(2020, 5, 31)

def iter_dates(from_date: datetime,
               to_date: datetime,
               step: timedelta) -> Iterator[Tuple[datetime]]:
    one_day = timedelta(days=1)
    date = from_date
    while date < to_date:
        yield date, date + step - one_day
        date += step

def parse_debt_files_non_mp():
    
    debt_files = fetch_data.get_debt_files()
    tracker = analyse_data.init_tracker()
    for fpath in debt_files:
        analyse_data.parse_file(fpath, tracker)
    return tracker

def parse_file(fpath: str,
                tracker_list: ListProxy,
                libors_list: ListProxy,
                non_libors_list: ListProxy,
                libors: Optional[Tuple[dict]] = None,
                non_libors: Optional[dict] = None):
    libors = libors or get_libors()
    non_libors = non_libors or get_non_libors()
    tracker = analyse_data.init_tracker()
    
    analyse_data.parse_file(fpath, tracker, libors, deepcopy(non_libors))
    tracker_list.append(tracker)
    libors_list.append(libors)
    non_libors_list.append(non_libors)

def parse_multi_files(files,
                      libors: Optional[Tuple[dict]] = None,
                      non_libors: Optional[dict] = None) -> Tuple[ListProxy]:
    
    # Shaves about 1 minute (~33%) off time vs non-multiprocessing
    
    manager = mp.Manager()
    pool = mp.Pool(processes=len(files))

    #tracker = manager.dict(analyse_data.init_tracker())
    tracker_list = manager.list()
    libors_list = manager.list()
    non_libors_list = manager.list()

    pool.starmap(parse_file, 
                 ((fpath, tracker_list, libors_list, non_libors_list,
                   libors, non_libors) for fpath in files))
    pool.close()
    pool.join()
    
    agg_trackers = analyse_data.aggregate_trackers(tracker_list)
    
    # Each process will have accumulated additional names to associate with
    # each benchmark rate.  Collect these together so we can record them (and
    # sense-check them).
    agg_libors = libors_list[0]
    for _libors in libors_list[1:]:
        for i, j in zip(agg_libors, _libors):
            i['names'] |= j['names']
    agg_non_libors = non_libors_list[0]
    for non_libor in non_libors_list[1:]:
        for bm_name in non_libor:
            agg_non_libors[bm_name]['names'] |= non_libor[bm_name]['names']
    
    return agg_trackers, agg_libors, agg_non_libors


def build_csv():
    for from_date, to_date in iter_dates(START_DATE, END_DATE):
        pass #TODO

def main(args):
    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv))
