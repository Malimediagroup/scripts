#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  invoke_mj_to_s3.py
#
#  Copyleft 2017 Maarten De Schrijver
#  <maarten de schrijver in the gmail domain dot com>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program. If not, see <http://www.gnu.org/licenses/>.
#
#
#######################################################################
#
#  invoke_mj_to_s3.py
#
#  AWS EC2-function to:
#
#   - invoke the mj_to_s3-fn's
#
#  This process will invoke 1 λ for every MAX_LIMIT (1000) resources
#  In such a way that no more than payload['MaxCallsPerMin'] (300 - 600)
#  would reach Mailjet.
#
#  If we get the resource 'contact', 'contactdata' and 'listsrecipient'
#  a total of about 5.1 milj resources should be fetched and thus
#  5.1k calls be made. A rato of 300 calls/minute this would take about
#  17 minutes.
#  
#######################################################################

import os
import sys
import logging
import json
import time
import argparse
import random
from base64 import b64decode
# Third party imports
import boto3
from botocore.client import Config
# Import Mailjet client when we can get a count on a resource
from mailjet_rest import Client
from mailjet_rest.client import ApiError

# Py2 and 3
try:
   input = raw_input
except NameError:
   pass

# Get logger
log = logging.getLogger('invoke_mj_to_s3')
log.setLevel(logging.DEBUG)
# create handler and set level to debug
ch = logging.StreamHandler(stream=sys.stdout)
#~ ch = logging.FileHandler('./logging.log', mode='a')
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
log.addHandler(ch)

# Some vars
fn_arn      = os.environ['FN_ARN']
MAX_LIMIT   = 1000 # Hard upper limit on the amount of resources fetchable in one call

# Lambda Boto3 Client
lambda_client = boto3.client('lambda', region_name='eu-central-1')
# Mailjet Client
MJ_APIKEY_PUBLIC    = os.environ['MJ_APIKEY_PUBLIC']
MJ_APIKEY_PRIVATE   = os.environ['MJ_APIKEY_PRIVATE']
mj_client = Client(auth=(MJ_APIKEY_PUBLIC, MJ_APIKEY_PRIVATE))

def get_total_number_in_resource(account, resource):
    """Get the total number of this resource and return it rounded to
       the nearest upper MAX_LIMIT."""
    filters = {'countOnly': '1'}
    res = getattr(mj_client, resource).get(filters=filters)
    return res.json()['Total']

def calculate_interval(calls_per_min, uniform_random=False):
    fixed_sleep_time = 60/float(calls_per_min)
    delta = fixed_sleep_time / 2
    if uniform_random:
        return random.uniform(fixed_sleep_time - delta, fixed_sleep_time + delta)
    return fixed_sleep_time

def make_oa_tuples(total):
    """How many repetitions and thus, how many times do we need to
       invoke the lambdafunction to iterate over the total number of
       contacts in batches of size batch_size.
       Return tuples like so:
        [(offset, amount), (offset, amount), ...]
        [(0, 50k), (50k, 50K), (100k, 50k), ...]
    """
    dm = divmod(total, MAX_LIMIT)
    rep = dm[0] + int(bool(dm[1]))
    return [(i*MAX_LIMIT, MAX_LIMIT) for i in range(rep)]

def make_fn_payload(payload, oa_tuple):
    d = {
        "PublicKeyEV"   : "MJ_MAIN_APIKEY_PUBLIC",
        "PrivateKeyEV"  : "MJ_MAIN_APIKEY_PRIVATE",
        "Resource"      : payload['Resource'],
        "Offset"        : oa_tuple[0],
        "Amount"        : oa_tuple[1],
        'InvokerPID'    : os.getpid(),
        #~ "List"      : None
    }
    return json.dumps(d).encode()

def invoke_mj_to_s3(payload):
    """Invoke the given λ-function with the given payload.
       We don't wait for the response."""
    response = lambda_client.invoke(
        FunctionName    = fn_arn,
        InvocationType  = 'Event',
        Payload         = payload,
    )
    return response


def lambda_handler(payload, cmd_args):
    auto = True
    total = get_total_number_in_resource(payload['Account'],
                                         payload['Resource'])
    log.info('Total number of resource "%s" in account is: %s' % (payload['Resource'], total))
    oa_tuples = make_oa_tuples(total)
    interval_seconds = calculate_interval(payload['MaxCallsPerMin'])
    log.info('Will invoke %s times λ-fn mj_to_s3 to get "%s".',
             len(oa_tuples), payload['Resource'])
    log.info('With max %s calls/minute, this will take about %s minutes.',
             payload['MaxCallsPerMin'], round(len(oa_tuples)/payload['MaxCallsPerMin']))
    log.info('Will sleep %s seconds, on average, between invocations.', interval_seconds)
    if not cmd_args.auto:
        r = input('Continue? (y/n) ')
        if r != 'y': exit(0)
    i = 0
    for oa_tuple in oa_tuples:
        i += 1
        pl = make_fn_payload(payload, oa_tuple)
        log.info('Iteration %s with payload: %s.', i, pl)
        if not payload['DryRun']:
            response = invoke_mj_to_s3(pl)
            log.debug('StatusCode was: %s', response['StatusCode'])
        else:
            log.info('DryRun: no fn invoked. Iteration %s.', i)
        slptime = calculate_interval(payload['MaxCallsPerMin'], uniform_random=True)
        log.info('Sleeping for %s secs...', slptime)
        time.sleep(slptime)


def main(cmd_args):
    payload = {
        'Account'       : None,                         # Which account
        'Resource'      : cmd_args.resource,
        'MaxCallsPerMin': cmd_args.max_calls_per_min,
        'InvokerPID'    : os.getpid(),                  # We send along our own PID, so the last lambda can tell us to stop
        'DryRun'        : False,                        # DryRun?
    }
    lambda_handler(payload, cmd_args)


if __name__ == '__main__':
    # Parse the command line
    parser = argparse.ArgumentParser(description="""Invoke a series of Lambda
        fn's to fetch resources from Mailjet.""")
    parser.add_argument('-r', '--resource', dest='resource', required=True,
                        choices=['contact', 'contactdata', 'listrecipient'],
                        help='Which resource to fetch.')
    parser.add_argument('-m', '--max-calls-per-min', dest='max_calls_per_min',
                        required=False, default=100, type=int,
                        help='How many calls per minute.')
    parser.add_argument('-u', '--uniform-random', dest='uniform_random',
                        required=False, action='store_true',
                        default=True, help='Randomize sleep interval.')
    parser.add_argument('-a', '--auto', dest='auto', action='store_true',
                        default=False, help='No questions asked.')
    cmd_args = parser.parse_args()
    main(cmd_args)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
