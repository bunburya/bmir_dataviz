#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Functions for fetching the FIRDS XML files from ESMA's website.""" 

import logging
from zipfile import ZipFile
from io import BytesIO
from datetime import datetime, timedelta
from os import mkdir, listdir, remove
from os.path import join, exists, dirname, realpath
from typing import Optional, List, Dict, Sequence, Tuple

import requests
from lxml import etree

from benchmark_data import libors, non_libors


FIRST_FIRDS_DATE = datetime(2017, 10, 15)  # Apparently the earliest date on which there are any FIRDS files.
DATA_DIR = 'data_files'
Q_URL = ('https://registers.esma.europa.eu/solr/esma_registers_firds_files/'
        'select?q=*&fq=publication_date:%5B{from_year}-{from_month}-'
        '{from_day}T00:00:00Z+TO+{to_year}-{to_month}-{to_day}T23:59:59Z%5D'
        '&wt=xml&indent=true&start={start}&rows={rows}')
FNAME_START = 'FULINS_{}'


def _request_file_urls(from_date: datetime, to_date: datetime, start, rows) -> etree._Element:
    
    url = Q_URL.format(
        from_year=from_date.year,
        from_month=from_date.month,
        from_day=from_date.day,
        to_year=to_date.year,
        to_month=to_date.month,
        to_day=to_date.day,
        start=start,
        rows=rows
    )
    response = requests.get(url)
    response.raise_for_status()
    return etree.fromstring(response.content)

def _parse_file_urls(root: etree._Element, urls: dict, ftype: str = ''):
    
    for entry in root[1]:
        fname = entry.xpath('.//str[@name="file_name"]')[0].text
        if (not ftype) or any(fname.startswith(FNAME_START.format(f)) for f in ftype):
            url = entry.xpath('.//str[@name="download_link"]')[0].text
            date = datetime.strptime(entry.xpath('.//date[@name="publication_date"]')[0].text.split('T')[0], '%Y-%m-%d')
            if date in urls:
                urls[date].append(url)
            else:
                urls[date] = [url]

def get_file_urls(from_date: datetime = None, to_date: datetime = None, ftype: str = '') -> Dict[datetime, List[str]]:
    
    start = 0
    rows = 100
    
    if from_date is None:
        to_date = datetime.today()
        from_date = to_date - timedelta(weeks=1)
    elif to_date is None:
        to_date = from_date
    
    logging.info('Getting files of type "{}" from {} to {}.'.format(ftype,
            from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d')))
        
    root = _request_file_urls(from_date, to_date, start, rows)
    num_results = int(root[1].attrib['numFound'])
    logging.info('Returned XML has {} results out of {} total.'.format(len(root[1]), num_results))
    urls = {}
    _parse_file_urls(root, urls, ftype)
    
    while num_results > (start + rows):
        start += rows
        logging.info('Getting results {}-{} of {}.'.format(start, start+rows, num_results))
        root = _request_file_urls(from_date, to_date, start, rows)
        _parse_file_urls(root, urls, ftype)
    
    return urls
    
def download_zipped_file(url: str, to_dir: str = None) -> str:
    if to_dir is None:
        to_dir = DATA_DIR
    response = requests.get(url)
    response.raise_for_status()
    zipfile = ZipFile(BytesIO(response.content))
    name = zipfile.namelist()[0]
    zipfile.extractall(path=to_dir)
    return join(to_dir, name)
    
def download_xml_files(from_date: datetime = None, to_date: datetime = None,
                       to_dir: str = None, ftype: str = '') -> Tuple[List[str], Optional[Sequence[datetime]]]:
    fpaths = []
    urls = get_file_urls(from_date, to_date, ftype=ftype)
    for date in urls:
        for fpath in urls[date]:
            fpaths.append(download_zipped_file(fpath, to_dir))
    return fpaths, sorted(urls)
    
def get_xml_files(ftype: str = '', data_dir: Optional[str] = None,
                    from_date: Optional[datetime] = None, 
                    to_date: Optional[datetime] = None) -> Tuple[List[str], Optional[Sequence[datetime]]]:
    logging.info('Getting FIRDS XML files.')
    if data_dir is None:
        data_dir = DATA_DIR
    xml_files = [join(data_dir, f) for f in listdir(data_dir) if (f.startswith(FNAME_START.format(ftype)) and f.endswith('.xml'))]
    if (not xml_files) or (from_date is not None):
        for f in xml_files:
            remove(f)
        return download_xml_files(to_dir=data_dir, ftype=ftype,
                                    from_date=from_date, to_date=to_date)
    else:
        return xml_files, None

def get_debt_files() -> List[str]:
    
    return get_xml_files(ftype='D')
