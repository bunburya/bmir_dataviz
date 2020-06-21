#!/usr/bin/env python
# coding: utf-8

"""Functions for parsing the FIRDS data, including determining which
benchmark interest rate (if any) is used.
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Tuple, Union, Optional, Iterable

from dateutil import parser as dateparser
from dateutil.tz import tzutc

from lxml import etree

import benchmark_data

TODAY_UTC = datetime.utcnow().replace(tzinfo=tzutc())
TODAY = datetime.today()
ZERO_TIME = timedelta()

# For testing purposes only
TO_CHECK = {
    'SONA',
    'SFOR',
    'LONDON INTERBANK MARKET',
    '1DAY USD SECURED OVERNIGH',
    'OFFERED RATE'
}


### Structure of a FIRDS XML file
# See:
# https://www.esma.europa.eu/sites/default/files/library/esma65-11-1193_firds_reference_data_reporting_instructions_v2.1.pdf
#
# XML structure (ignoring irrelevant nodes):
# - BizData (root)
#   - Hdr
#   - Pyld (root[1])
#     - Document (Pyld[0])
#       - FinInstrmRptgRefDataRpt (Document[0])
#         - RptHdr
#         - RefData (FinInstrmRptgRefDataRpt[1:]) (repeats for each ISIN)
#           - FinInstrmGnlAttrbts (RefData[0])
#             - Id (FinInstrmGnlAttrbts[0]): text = ISIN
#             - NtnlCcy (FinInstrmGnlAttrbts[4]): text = notional currency
#           - Issr (RefData[1]): text = issuer LEI
#           - TradgVnRltdAttrbts (RefData[2])
#             - Id (TradgVnRltdAttrbts[0]): text = trading venue MIC
#           - DebtInstrmAttrbts (RefData[3])
#             - TtlIssdNmnlAmt (DebtInstrmAttrbts[0]): text = total issued nominal amount
#             - MtrtyDt (DebtInstrmAttrbts[1]): text = maturity date in '%Y-%m-%d' format
#             - NmnlValPerUnit (DebtInstrmAttrbts[2]): text = minimum denomination
#             - IntrstRate (DebtInstrmAttrbts[3]):
#               - Fxd: text = interest rate (or None)
#               OR
#               - Fltg
#                 - RefRate (Fltg[0])
#                   - Nm (RefRate[0]): text = reference rate name
#                   OR
#                   - ISIN (RefRate[0]): text = ISIN
#                   OR
#                   - Indx (RefRate[0]): text = index (?)
#                   
#                 - Term (Fltg[1])
#                   - Unit (Term[0]): text = unit of time (eg, 'MNTH')
#                   - Val (Term[1]): text = term as number of units
#                 - BsisPtSprd (Fltg[2]): text = basis point spread
#           - TechAttrbts (RefData[4])
#             - RlvntCmptntAuthrty (TechAttrbts[0]): text = country of RCA
#
# See also:
# https://www.esma.europa.eu/sites/default/files/library/esma70-1861941480-56_qas_mifir_data_reporting.pdf
###

day = timedelta(days=1)

def find_by_isin(isin: str, elems: Iterable[etree._Element]) -> etree._Element:
    for e in elems:
        if get_isin(e) == isin:
            return e

def get_isin(elem: etree._Element) -> str:
    return elem[0][0].text
      
def get_maturity_date(elem:etree._Element) -> datetime:
    return datetime.strptime(elem[3][1].text, '%Y-%m-%d')
    
def get_maturity(elem: etree._Element, from_date: datetime = TODAY) -> float:
    maturity = ((get_maturity_date(s) - from_date) / day) / 354.25
    return maturity

def get_tv_dates(elem: etree._Element) -> Tuple[datetime, datetime]:
    tv_data = elem[2]
    first_trade = None
    termination = None
    for datum in tv_data:
        if datum.tag.endswith('}FrstTradDt'):
            first_trade = dateparser.isoparse(datum.text)
        elif datum.tag.endswith('}TermntnDt'):
            termination = dateparser.isoparse(datum.text)
    return first_trade, termination

def get_nominal_amount(elem: etree._Element) -> float:
    return float(elem[3][0].text)

def get_currency(elem: etree._Element, convert_DEM: bool = True) -> str:
    currency = elem[0][4].text
    if (currency == 'DEM') and convert_DEM:
        return 'EUR'
    else:
        return currency

def get_interest_rate(elem: etree._Element) -> dict:
    ir_elem = elem[3][3][0]
    ir_data = {}
    if ir_elem.tag.endswith('}Fxd'):
        ir_data['fixed_floating'] = 'fixed'
        ir_data['rate'] = ir_elem.text
    elif ir_elem.tag.endswith('}Fltg'):
        ir_data['fixed_floating'] = 'floating'
        ref_rate = ir_elem[0]
        if ref_rate[0].tag.endswith('}Nm'):
            ir_data['index_name'] = ref_rate[0].text
        elif ref_rate[0].tag.endswith('}ISIN'):
            ir_data['index_isin'] = ref_rate[0].text
        elif ref_rate[0].tag.endswith('}Indx'):
            ir_data['index_code'] = ref_rate[0].text
        ir_data['term'] = (ir_elem[1][0].text, float(ir_elem[1][1].text))
        ir_data['spread'] = float(ir_elem[2].text)
    else:
        raise ValueError('Found unexpected interest rate: {}.'.format(ir_elem.tag))
    return ir_data

def is_floating(elem: etree._Element) -> bool:
    return get_interest_rate(elem)['fixed_floating'] == 'floating'

def print_details(elem: etree._Element, float_only: bool = False):
    if not float_only:
        if get_interest_rate(elem)['fixed_floating'] != 'floating':
            return
    print('ISIN: {}'.format(get_isin(elem)))
    print('Nominal amount: {} {}'.format(get_currency(elem), get_nominal_amount(elem)))
    print('Maturity: {} years'.format(get_maturity(elem)))
    print('Interest rate: {}'.format(get_interest_rate(elem)))


currency_mismatch = {}
def is_benchmark(bm_data: dict, ir_data: dict, check_code: bool = True) -> Tuple[bool, Optional[str]]:
    name = ir_data.get('index_name')
    code = ir_data.get('index_code')
    isin = ir_data.get('index_isin')
    
    if check_code and (code == bm_data['code']):
            return True, 'code'
    if isin in bm_data['isins']:
        return True, 'isin'
    if not name:
        return False, None
    name = name.upper()
    if (name in bm_data['names'] | bm_data['isins']) or (name == bm_data['code']):
        # Check if index_name is one of the benchmark's recognised names, or is one of the benchmark's
        # recognised ISINs or is the benchmark's code (the latter two happen sometimes)
        return True, 'name'
    if any(all(word.upper() in re.split('[ \-+]', name) for word in root_name) for root_name in bm_data['root_names']):
        bm_data['names'].add(name)
        return True, 'root_name'
    return False, None

def is_libor(ir_data: dict, currency: str, libors: Tuple[dict] = benchmark_data.libors) -> Tuple[Union[str, bool], Optional[str]]:
    if 'EURIBOR' in libors:
        print('is_libor', ir_data)
    for bm_data in libors:
        if bm_data['currency'] is None:
            check_code = True
        else:
            check_code = False
        is_match, match_type = is_benchmark(bm_data, ir_data, check_code)
        if is_match:
            bm_currency = bm_data['currency']
            if bm_currency is None:
                # Security has matched generic_libor, so we just guess LIBOR currency
                # from currency of security.
                return currency, match_type
            else:
                return bm_currency, match_type
    return False, None

def get_benchmark(ir_data: dict, currency: str, libors: Tuple[dict] = benchmark_data.libors,
                    non_libors: dict = benchmark_data.non_libors,
                    isin: str = None) -> Tuple[Optional[str], Optional[str]]:
    
    benchmark = None
    if 'EURIBOR' in libors:
        print('get_benchmark', libors == non_libors, benchmark_data.libors == benchmark_data.non_libors)
    libor_currency, match_type = is_libor(ir_data, currency, libors)
    if libor_currency:
        benchmark = ' '.join((libor_currency, 'LIBOR'))
        bm_currency = libor_currency
    else:
        for bm in non_libors:
            is_match, match_type = is_benchmark(non_libors[bm], ir_data, currency)
            if is_match:
                benchmark = bm
                bm_currency = non_libors[bm]['currency']
                break
    
    if benchmark is not None:
        if (isin is not None) and (bm_currency != currency):
            # Security has matched a specific currency LIBOR, but that does not match the
            # security's own currency (possibly indicates that one of them is wrong)
            currency_mismatch[isin] = (bm_currency, currency)
        return benchmark, match_type
    else:
        return None, None
    

TRACKER_PROTOTYPE = {
    'last_isin': None,
    'floating': 0,
    'fixed': 0,
    'floating_uncat': {
        'index_name': {},
        'index_code': {},
        'index_isin': {}
    },
    'benchmark_data': {bm: {
            'count': 0,
            'agg_maturity': timedelta(),
            'agg_nominal': 0  
        } for bm in benchmark_data.bm_names},
    'duplicates': 0,
    'matured': 0,
    'delisted': 0,
    'zero_nominal': 0
}

def init_tracker() -> dict:
    return TRACKER_PROTOTYPE.copy()

def aggregate_trackers(trackers: Iterable) -> dict:
    agg = init_tracker()
    for k in ('floating', 'fixed', 'duplicates', 'matured', 'delisted', 'zero_nominal'):
        agg[k] = sum(t[k] for t in trackers)
    for bm in agg['benchmark_data']:
        agg['benchmark_data'][bm]['count'] = sum(t['benchmark_data'][bm]['count'] for t in trackers)
        agg['benchmark_data'][bm]['agg_nominal'] = sum(t['benchmark_data'][bm]['agg_nominal'] for t in trackers)
        agg['benchmark_data'][bm]['agg_maturity'] = sum((t['benchmark_data'][bm]['agg_maturity'] for t in trackers),
                                                        start=ZERO_TIME)
    for t in trackers:
        for k in t['floating_uncat']:
            agg['floating_uncat'][k].update(t['floating_uncat'][k])
    return agg

def parse_security(s, tracker: dict, assess_date: datetime = TODAY_UTC, libors: Tuple[dict] = benchmark_data.libors,
                    non_libors: dict = benchmark_data.non_libors) -> None:
    _, term_date = get_tv_dates(s)
    if (term_date is not None) and (term_date < assess_date):
        tracker['delisted'] += 1
        return
    isin = get_isin(s)
    if isin == tracker['last_isin']:
        tracker['duplicates'] += 1
        return
    tracker['last_isin'] = isin
    maturity = get_maturity(s, from_date=assess_date)
    if maturity < ZERO_TIME:
        tracker['matured'] += 1
        return
    nominal_amount = get_nominal_amount(s)
    if nominal_amount == 0.0:
        tracker['zero_nominal'] += 1
        return
    ir_data = get_interest_rate(s)
    currency = get_currency(s)
    if ir_data['fixed_floating'] == 'floating':
        tracker['floating'] += 1
        bm, match_type = get_benchmark(ir_data, currency, libors, non_libors)
        if bm:
            tracker['benchmark_data'][bm] += 1
            tracker['benchmark_data'][bm]['agg_maturity'] += maturity
            tracker['benchmark_data'][bm]['agg_nominal'] += nominal_amount
        else:
            for identifier in ('index_isin', 'index_name', 'index_code'):
                if identifier in ir_data:
                    tracker['floating_uncat'][identifier][ir_data[identifier]] = tracker['floating_uncat'][identifier].get(ir_data[identifier], 0) + 1
    else:
        tracker['fixed'] += 1

def parse_file(fpath, tracker: dict, libors: Tuple[dict] = benchmark_data.libors,
                non_libors: dict = benchmark_data.non_libors) -> None:
    if 'EURIBOR' in libors:
        print('parse_file', fpath, libors == non_libors)
    for event, elem in etree.iterparse(fpath):
        if elem.tag.endswith('}RefData'):
            parse_security(elem, tracker, libors, non_libors)
            elem.clear()
