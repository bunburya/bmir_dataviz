#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The main script for scraping the FIRDS XML data.  Contains functions
for iterating through each week in a reference time period, fetching the
relevant XML files, analysing the contents and building a CSV file with
aggregated data about the benchmark rates.
"""

import multiprocessing as mp
import csv
import logging

from multiprocessing.managers import ListProxy
from copy import deepcopy
from typing import Optional, Tuple, Iterator
from datetime import datetime, timedelta
from os import makedirs
from os.path import join, expanduser, exists

import fetch_data
import analyse_data

from benchmark_data import benchmark_names, get_libors, get_non_libors

START_DATE = datetime(2018, 1, 1)
END_DATE = datetime(2020, 5, 31)
OUTPUT_DIR = join(expanduser('~'), 'data', 'bmir_data')
REPORTS_DIR = join(OUTPUT_DIR, 'reports')
if not exists(REPORTS_DIR):
    makedirs(REPORTS_DIR)
CSV_FILE = join(OUTPUT_DIR, 'bmir_data.csv')

INITIAL_LIBORS = get_libors()
INITIAL_NON_LIBORS = get_non_libors()

LABELS = ('COUNT', 'AVERAGE MATURITY', 'WEIGHTED AVERAGE MATURITY', 'AVERAGE NOMINAL AMOUNT')
CSV_HEADINGS = ['DATE']

# NB:  The below code should generate labels for benchmark names in the same order as benchmark_data
for name in benchmark_names:
    for label in LABELS:
        CSV_HEADINGS.append('{} - {}'.format(label, name))

#for libor in INITIAL_LIBORS:
#    for label in LABELS:
#        CSV_HEADINGS.append('{} - {} LIBOR'.format(label, libor['currency']))
#for non_libor in INITIAL_NON_LIBORS:
#    for label in LABELS:
#        CSV_HEADINGS.append('{} - {}'.format(label, non_libor))

def iter_dates(from_date: datetime,
               to_date: datetime,
               step: timedelta = timedelta(days=7)) -> Iterator[Tuple[datetime]]:
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
    
    agg_tracker = analyse_data.aggregate_trackers(tracker_list)
    
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
    
    return agg_tracker, agg_libors, agg_non_libors

def save_report(date: datetime, tracker: dict,
                libors: Tuple[dict], non_libors: dict) -> str:
    gen_time = datetime.today()
    fname = '{}.log'.format(date.strftime('%Y-%m-%d'))
    fpath = join(REPORTS_DIR, fname)
    with open(fpath, 'w') as f:
        f.write('Report for parsing FIRDS XML data from {}.'
                    'Generated on {}.\n'.format(
                        date.strftime('%d-%m-%Y'),
                        gen_time.strftime('%d-%m-%Y at %H:%M')))
        f.write('\n')
        f.write('Uncategorised floating interest rates:\n')
        f.write('  Names:\n')
        for name in tracker['floating_uncat']['index_name']:
            f.write('    {}\n'.format(name))
        f.write('  ISINs:\n')
        for isin in tracker['floating_uncat']['index_name']:
            f.write('    {}\n'.format(isin))
        f.write('  Codes:\n')
        for code in tracker['floating_uncat']['index_code']:
            f.write('    {}\n'.format(code))
        f.write('\n')
        f.write('LIBOR names:\n')
        for libor in libors:
            c = libor['currency'] or 'Generic'
            f.write('  {}:\n'.format(c))
            for name in libor['names']:
                f.write('    {}\n'.format(name))
        f.write('\n')
        f.write('Non-LIBOR benchmark rate names:\n')
        for non_libor in non_libors:
            f.write('  {}:\n'.format(non_libor))
            for name in non_libors[non_libor]['names']:
                f.write('    {}\n'.format(name))
    return fpath

def build_csv(new_file: bool = True, report: bool = True):
    run_time = datetime.today()
    logging.info('Beginning to build CSV on {}.'.format(run_time.strftime('%d-%m-%Y at %H:%M')))
    tracker = analyse_data.init_tracker()
    libors = INITIAL_LIBORS
    non_libors = INITIAL_NON_LIBORS
    with open(CSV_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        if new_file:
            f.truncate()
            writer.writerow(CSV_HEADINGS)
            logging.info('Wrote header row to blank CSV file {}.'.format(CSV_FILE))
        else:
            logging.info('Continuing with existing CSV file {}.'.format(CSV_FILE))
        for from_date, to_date in iter_dates(START_DATE, END_DATE):
            from_date_hr = from_date.strftime('%d-%m-%Y')
            to_date_hr = to_date.strftime('%d-%m-%Y')
            logging.info('Searching for XML files in date range {} to {}.'.format(from_date_hr, to_date_hr))
            files, dates = fetch_data.get_xml_files(ftype='D', from_date=from_date, to_date=to_date)
            # Get actual date files were published
            if len(set(dates)) > 1:
                err_msg = 'XML files for date range {}-{} are from {} different dates: {}.'.format(
                    from_date_hr,
                    to_date_hr,
                    len(dates),
                    dates)
                logging.critical(err_msg)
                raise ValueError(err_msg)
            date = dates[0]
            logging.info('Found {} files from date {}'.format(len(files), date.strftime('%d-%m-%Y')))
            tracker, libors, non_libors = parse_multi_files(files)
            if report:
                report_path = save_report(date, tracker, libors, non_libors)
                logging.info('Report saved to {}.'.format(report_path))
            bm_data = tracker['benchmark_data']
            values = [date.strftime('%Y-%m-%d')]
            # NB:  Values must be appended in same order as LABELS.  
            for name in benchmark_names:
                count = bm_data[name]['count']
                agg_maturity = bm_data[name]['agg_maturity']
                agg_nominal = bm_data[name]['agg_nominal']
                agg_mxn = bm_data[name]['agg_mxn']
                values.append(count)
                try:
                    values.append(agg_maturity / count)
                except ZeroDivisionError:
                    values.append(0)
                try:
                    values.append(agg_mxn / agg_nominal)
                except ZeroDivisionError:
                    values.append(0)
                try:
                    values.append(agg_nominal / count)
                except ZeroDivisionError:
                    values.append(0)
            writer.writerow(values)
            logging.info('Wrote data to CSV file.')

def main(args):
    logging.getLogger().setLevel(20)
    build_csv()

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv))
