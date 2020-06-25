#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""This file contains data about the various benchmark interest rates,
which is used by the is_benchmark and is_libor functions in analyse_data
to check what benchmark rate (if any) is used by a given security.
"""

from typing import Tuple
from copy import deepcopy

libor_currencies = {'GBP', 'EUR', 'USD', 'CHF', 'JPY'}

libors = (
    
    {
        'root_names': (
            ('GBP', 'LIBOR'),
            ('STERLING', 'LIBOR'),
            ('GBR', 'LIBOR')
        ),
        'isins': {
            'GB00BD080045',
            'GB00BD07ZZ10',
            'GB0003117685',
            'GB0009655183'
        },
        'code': 'LIBO',
        'names': {
            'GBP LIBOR',
            'BP0003M',
            '3MTH GBP',
            'OFFERED GBP RATE',
            '1MTH GBP',
            '30DAY GBP',
            '6MTH GBP',
            '90DAY GBP',
            '180DAY GBP'
            
        },
        'currency': 'GBP'
    },
    
    {
        'root_names': (
            ('USD', 'LIBOR'),
            ('US', 'LIBOR'),
            ('U.S.$', 'LIBOR')
        ),
        'isins': {
            'GB00BD080714',
            'GB0003758389',
            'GB00BD080607',
            'GB00BD080821',
            'GB00BD080938',
            'GB0003766598',
            'GB0003764668',
            'GB00B5MPDP30',
            'GB00B5M93C29',
            'GB0003758058',
            'GB00B1316Y29',
            
        },
        'code': 'LIBO',
        'names': {
            'USD LIBOR',
            'US0003M',
            'OFFERED USD RATE',
            '1MTH USD',
            '30DAY USD',
            '3MTH USD',
            '6MTH USD',
            '180DAY USD',
            '90DAY USD'
        },
        'currency': 'USD'
    },
    
    {
        'root_names': (
            ('CHF', 'LIBOR'),
            ('SWISS', 'FRANC', 'LIBOR')
        ),
        'isins': {
            'GB00BD080F90',
            'GB00BD080C69',
        },
        'code': 'LIBO',
        'names': {
            'CHF LIBOR'
        },
        'currency': 'CHF'
    },
    
    {
        'root_names': (
            ('EUR', 'LIBOR'),
            ('EURO', 'LIBOR')
        ),
        'isins': {
            'GB00BD080482',
            'GB00BBD82B22',
            'GB0004356027',
            'GB00BBD82C39',
            'GB0004356795',
            'GB0004357090',
            'GB0004359369'
        },
        'code': 'LIBO',
        'names': {
            'EUR LIBOR',
            '3MTH EUR',
            '6MTH EUR',
            '180DAY EUR',
        },
        'currency': 'EUR'
    },
    
    {
        'root_names': (
            ('JPY', 'LIBOR'),
            ('YEN', 'LIBOR')
        ),
        'isins': set(),
        'code': 'LIBO',
        'names': {
            'JPY LIBOR'
        },
        'currency': 'JPY'
        
    },

    {
        'root_names': (
            ('LIBOR',),
        ),
        'isins': set(),
        'code': 'LIBO',
        'names': {
            'LIBOR',
            'LIBOR01',
            'LONDON INTERBANK MARKET'
        },
        'currency': None
    }

)

non_libors = {
    
    'EURIBOR': {
        'root_names': (
            ('EURIBOR',),
            ('EUR', 'INTERBANK', 'OFFERED'),
            ('EURIB',),
            ('EURIOBOR',)
        ),
        'isins': {
            'EU0009652783',
            'EU0009659937',
            'EU0009652791',
            'EU000A0X7136',
            'EU0009678507',
            'EU0005301658',
            'EU0009652841',
            'EU0009652890',
            'EU0009652809',
            'EU000A1L18K0',
            'EU000A1L18L8',
            'EU000A0X7128',
            'EU000A1L18V7',
            'EU000A0X7144',
            'EU000A1L18R5',
            'EU000A0X7151'
        },
        'code': 'EURI',
        'names': {
            'EURIBOR',
            'EURI',
            'EUROBOR',
            '1YR EUR',
            '180Day EUR'
        },
        'currency': 'EUR'
    },

    'EONIA': {
        'root_names': (
            ('EONIA',),
            ('EURO', 'OVERNIGHT', 'INDEX', 'AVERAGE')
        ),
        'isins': {
            'EU0009659945',
            'EU0009689967',
            'EU0009689975'
        },
        'code': 'EONA',
        'names': {
            'EONIA',
            'EURO OVERNIGHT INDEX AVERAGE'
        },
        'currency': 'EUR'
    },

    'SONIA': {
        'root_names': (
            ('SONIA',),
            ('STERLING', 'OVERNIGHT', 'INDEX', 'AVERAGE')
        ),
        'isins': {
            'GB00B56Z6W79'
        },
        'code': 'SONA',
        'names': {
            'SONIA',
            'STERLING OVERNIGHT INTERB'
        },
        'currency': 'GBP'
    },
    
    'TIBOR': {
        'root_names': (
            ('TIBOR',),
            ('TOKYO', 'INTERBANK', 'OFFERED'),
        ),
        'isins': set(),
        'code': 'TIBO',
        'names': {
            'TIBOR'
        },
        'currency': 'JPY'
    },
    
    'TONAR': {
        'root_names': (
            ('TONAR',),
            ('TOKYO', 'OVERNIGHT', 'AVERAGE')
        ),
        'isins': set(),
        'code': False,
        'names': {
            'TONAR'
        },
        'currency': 'JPY'
    },

    'ESTR': {
        'root_names': (
            ('ESTR',),
            ('EURO', 'SHORT', 'TERM', 'RATE'),
            ('EURO', 'SHORT-TERM', 'RATE')
        ),
        'isins': {
            'EU000A2X2A25'
        },
        'code': 'ESTR',
        'names': {
            '€STR',
        },
        'currency': 'GBP'
    },

    'SOFR': {
        'root_names': (
            ('SOFR',),
            ('SECURED', 'OVERNIGHT', 'FINANCING', 'RATE')
        ),
        'isins': set(),
        'code': 'SOFR',
        'names': {
            'SOFR',
            'SECURED OVERNIGHT FINANCING RATE',
            '1DAY USD SECURED OVERNIGH'
        },
        'currency': 'USD'
    },

    'SARON': {
        'root_names': (
            ('SARON',),
            ('SWISS AVERAGE RATE OVERNIGHT')
        ),
        'isins': {
            'CH0049613687'
        },
        'code': False,
        'names': {
            'SARON',
            'SWISS AVERAGE RATE OVERNIGHT'
        },
        'currency': 'CHF'
    },
}

benchmark_names = [' '.join((bm['currency'], 'LIBOR')) for bm in libors if bm['currency'] is not None] \
                    + sorted(non_libors.keys())

replacements = {
    'GBP LIBOR': 'SONIA',
    'USD LIBOR': 'SOFR',
    'CHF LIBOR': 'SARON',
    'EUR LIBOR': 'ESTR',
    'EURIBOR': 'ESTR',
    'EONIA': 'ESTR'
}

def get_libors() -> Tuple[dict]:
    return deepcopy(libors)

def get_non_libors() -> dict:
    return deepcopy(non_libors)
