#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  change_email.py
#
#  Copyleft 2017 Mali Media Group
#  <http://malimedia.be>
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
###############################################################################
#
#  change_email.py
#  
#  Single-file command line script to change a user's emailaddress in the
#  various places where it is present.
#  
#  Various keys need to be in the environment:
#
#       $ source env.secrets
#
#  AWS credentials (for DynamoDB) in the usual place.
#  
#  # Change emailadres
#  ===================
#  
#  Where is a user's emailaddress present?
#  - DDB:
#      - Emails    (add ipv change)
#      - Contacts  (change)
#  - MySQL:
#      - Contacts  (change + split/canonicalize)
#  - MailJet:
#      - Check All (Bdm) Accounts: Main and Transactional (Extern)
#          -> unsubscribe/remove from all lists + subscribe new address with
#             all contactdata (uuid, fname, lname, ... seg_num, block)
#  - Bdm:
#      - change_script: os.environ['BDM_URL_CHANGE']
#  
#  - Odoo:
#      - res.partner
#  - Zendesk:
#      - TBI
#  
#  # History for "objects"
#  =======================
#  
#  - TimeStamp
#  - UUID
#  - Delta: [
#      - Value
#      - From
#      - To
#      ]
#
###############################################################################

# System imports
import os
import logging
import argparse
import hashlib
import base64
try:
    import xmlrpclib
except ImportError as e:
    import xmlrpc.client as xmlrpclib
from datetime import datetime
from pprint import pprint
from copy import deepcopy

# Third party imports
import pymysql
import requests
import boto3
from botocore.client import Config
from mailjet_rest import Client

# Py2 and 3
try:
   input = raw_input
except NameError:
   pass

# Get logger
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
log.addHandler(ch)

# Some constants
BDM_URL_CHANGE    = os.environ['BDM_URL_CHANGE']
BDM_URL_GET_ID    = os.environ['BDM_URL_GET_ID']
BDM_URL_GET_EMAIL = os.environ['BDM_URL_GET_EMAIL']
EM_QUESTION = """Old email "%(old_email)s" found in Biedmee. Associated Clang ID is: %(id)s.
Will be changed to: "%(new_email)s". Continue? (y)es / (n)o : """
SUB_QUESTION = """What to do?
(1) Add and subscribe existing Mailjet contact to list Biedmee.be
(2) Add/update anyway with new data + sub to list
(3) Nothing
"""
UPDATE_QUESTION = """Update %(system)s? (y)es / (n)o : """
MJ_LIST_ID          = 1805018
MJ_APIKEY_PUBLIC    = os.environ['MJ_APIKEY_PUBLIC']
MJ_APIKEY_PRIVATE   = os.environ['MJ_APIKEY_PRIVATE']
MYSQL = {
    'type'        : 'mysql',
    'host'        : os.environ['MYSQL_HOST'],
    'port'        : 3306,
    'db_name'     : 'mmgmysqldb',
    'db_username' : 'mmgmysqluser',
    'db_password' : os.environ['MYSQL_DB_PASSWORD'],
    'table_name'  : 'Contacts',
}
CAMPAIGN_URL = os.environ['CAMPAIGN_URL']
CAMPAIGN_API = os.environ['CAMPAIGN_API']

# Init Mailjet client
mailjet = Client(auth=(MJ_APIKEY_PUBLIC, MJ_APIKEY_PRIVATE))

# Init MySQL conn
db_conn = pymysql.connect(MYSQL['host'],
    port=MYSQL['port'],
    user=MYSQL['db_username'],
    passwd=MYSQL['db_password'],
    db=MYSQL['db_name'],
    charset='utf8',
    connect_timeout=5)

# Initialize boto3-clients per container
region_name = 'eu-central-1'
ddb_client  = boto3.client('dynamodb',
                region_name=region_name, 
                endpoint_url="https://dynamodb.eu-central-1.amazonaws.com")

utc_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def send_to_API(data):
    """Send all received data to the 'Baseline_Update_Email'-lambda."""
    headers = {'Content-Type': 'application/json', 'X-Api-Key': CAMPAIGN_API}
    data    = {"data": data}
    r = requests.post(CAMPAIGN_URL, headers=headers, json=data)
    log.debug('Code: %s, Text: %s', r.status_code, r.text)
    return r

def update_ddb(email):
    # Simply add here
    response = ddb_client.put_item(
        TableName='Emails',
        Item={
            'Email': {'S': email.lower(),},
            'TimeStamp': {'S': utc_timestamp}
        }
    )
    log.debug(response)
    # Look up and change
    #~ table = ddb_client.Table('Contacts')

def mysql_get(email):
    sql = """SELECT c.uuid,
       c.email_cleaned,
       cam.short_name,
       cam.campaign_decimal,
       cc.created_at,
       cc.source_ipv4
  FROM Contacts c
       JOIN ContactsCampaigns cc
         ON c.uuid = cc.contact_uuid
       JOIN Campaigns cam
         ON cc.campaign_uuid = cam.uuid
 WHERE c.email_cleaned = %s
 ORDER BY cc.created_at ASC;"""
    with db_conn.cursor() as cursor:
        cursor.execute(sql, (email, ))
        res = cursor.fetchall()
    return res

def warn_subscription(mj_contact):
    if mj_contact['Subscriptions']:
        l = [x['IsUnsubscribed'] for x in mj_contact['Subscriptions'] if x['ListID'] == MJ_LIST_ID]
        if l and not l[0]:
            pass
        else:
            log.warn('This contact has UNSUBSCRIBED from Biedmee.be.')
    else:
        log.warn('This contact is NOT subscribed to any lists.')

def mailjet_get(contact_id_or_email, with_data=True, with_subscriptions=True):
    """Get the contact data for this ID or email.
    Defaults to getting ALL the data + subscriptions."""
    result = mailjet.contact.get(id=contact_id_or_email)
    if result.status_code == 200:
        res = result.json()['Data'][0]
        if with_data:
            result = mailjet.contactdata.get(id=contact_id_or_email)
            res['ContactData'] = result.json()['Data'][0]['Data']
        if with_subscriptions:
            filters={'Contact': res["ID"]}
            result = mailjet.listrecipient.get(filters=filters)
            res['Subscriptions'] = list()
            res['Subscriptions'].extend(result.json()['Data'])
        return res
    # Contact not found
    elif result.status_code == 404:
        return False
    # Error during GET: bad emailaddress?
    elif result.status_code == 400:
        log.warn('Status: %s - Reason: %s', result.status_code, result.reason)
        return False
    
def mailjet_subaction(contact, action, list_id=0):
    assert action in ('addforce', 'addnoforce', 'remove', 'unsub'), 'Action not known.'
    actions_list = list()
    if not list_id:
        for contact_list in contact['Subscriptions']:
            actions_list.append({"ListID": contact_list["ListID"],
                                 "Action": action})
    else:
        actions_list.append({"ListID": list_id, "Action": action})
    data = {'ContactsLists': actions_list}
    result = mailjet.contact_managecontactslists.create(id=contact["ID"],
                                                        data=data)
    return result

def mailjet_add(email, props):
    data = {
      'Email': email,
      'Name': ' '.join([props['firstname'], props['lastname']]),
      'Action': 'addnoforce',
      'Properties': props
    }
    print(data)
    result = mailjet.contactslist_managecontact.create(id=MJ_LIST_ID, data=data)
    return result

def confirm_mj_props(mj_contact):
    d = dict()
    props = [
        ('firstname', str),
        ('lastname', str),
        ('language', str),
        ('gender', str),
        ('dob', str),
        ('optinorigin', float),
        ('optinip', str),
        ('ezine_frequency', str),
        ('seg_num', int),
        ('uuid', str),
        ('block', int),
    ]
    if mj_contact:
        props_flat = {p['Name']: p['Value'] for p in mj_contact['ContactData']}
    else:
        props_flat = {}
    for prop, prop_type in props:
        try:
            suggest = prop_type(props_flat[prop])
        except KeyError as e:
            suggest = 'no value'
        answer = input('Value for %(prop)s ? (Enter for "%(suggest)s"): ' % \
                        {'prop': prop, 'suggest': suggest})
        if not answer:
            try:
                d[prop] = prop_type(props_flat[prop])
            except KeyError as e:
                pass
        else:
            d[prop] = prop_type(answer)
    return d

def update_mailjet(old_email, new_email, props):
    # Find one (or both) accounts
    contact1 = mailjet_get(old_email)
    contact2 = mailjet_get(new_email)
    pprint({old_email: contact1})
    pprint({new_email: contact2})
    # Unsub/remove old email
    if contact1 and contact1['Subscriptions']:
        log.info('Removing "%s" from lists in Mailjet.', old_email)
        res = mailjet_subaction(contact1, 'remove')
        log.debug(res)
    elif contact1:
        log.info('Mailjet contact "%s" was found but did not have any subscriptions.', old_email)
    else:
        log.info('No Mailjet contact "%s" was found.', old_email)
    # Sub/add new email
    if not contact2:
        r = mailjet_add(new_email, props)
        log.debug('Code: %s, Text; %s', r.status_code, r.text)
    else:
        log.info('Mailjet contact "%s" already there.', new_email)
        log.info('Subscriptions are: %s', contact2['Subscriptions'])
        choice = input(SUB_QUESTION)
        if int(choice) == 1:
            log.info('Subscribing only...')
            mailjet_subaction(contact2, 'addforce', MJ_LIST_ID)
        elif int(choice) == 2:
            log.info('Adding and subscribing...')
            r = mailjet_add(new_email, props)
            log.debug('Code: %s, Text; %s', r.status_code, r.text)
        else:
            log.info('Skipping')

def update_odoo(old_email, new_email):
    url         = os.environ['ERP_URL']
    db          = 'mmg_odoo_v9_db'
    username    = os.environ['ERP_USERNAME']
    password    = os.environ['ERP_PASSWD']
    common      = xmlrpclib.ServerProxy('{}/xmlrpc/2/common'.format(url))
    uid         = common.authenticate(db, username, password, {})
    conn        = xmlrpclib.ServerProxy('{}/xmlrpc/2/object'.format(url))
    res = conn.execute_kw(db, uid, password,
        'res.partner', 'search_read', [[ ('email', 'ilike', old_email) ]],
        {'fields': ['id', 'clang_id', 'name', 'display_name', 'email',
                    'create_date', 'write_date'], 'limit': 10}
    )
    if res:
        pprint(res)
        if len(res) > 1:
            log.warn('More then one contact in Odoo. What to do?')
        else:
            log.info('Updating Odoo contact/partner "%s" with ID: %s.', old_email, res[0]['id'])
            res = conn.execute_kw(db, uid, password, 'res.partner', 'write', [[res[0]['id']], {
                'email': new_email
            }])
            log.debug(res)
    else:
        log.info('No contact found in Odoo with email "%s".', old_email)


def main(old_email, new_email):
    old_email_url = BDM_URL_GET_EMAIL % {'email': old_email}
    new_email_url = BDM_URL_GET_EMAIL % {'email': new_email}
    log.info('Looking up old email: %s.', old_email_url)
    try:
        r = requests.get(old_email_url)
    except requests.exceptions.ConnectionError as e:
        log.error(e)
        exit(1)
    else:
        if r.status_code == 200 and r.json():
            log.info('Found "%s" in BIEDMEE. Data is:', old_email)
            bdm_contact = r.json()
            bdm_id      = bdm_contact['ID']
            clang_id    = bdm_contact['clang_ID']
            pprint(bdm_contact)
        else:
            clang_id    = False
            log.warn('Emailaddress "%s" not found in BIEDMEE. User doesn\'t exist?', old_email)
            answer = input('Continue? (y)es / (n)o : ')
            if answer == 'n':
                exit()
    log.info('Looking up new email: %s.', new_email_url)
    try:
        r = requests.get(new_email_url)
    except requests.exceptions.ConnectionError as e:
        log.error(e)
    else:
        if r.status_code == 200 and r.json():
            pprint(r.json())
            log.warn('Provided NEW emailaddres "%s" already exists in BIEDMEE! Continue with care!', new_email)
            # We can't delete a user, so we can change the emailaddres into
            # a hex-encoded version to "disable" the account
            # (hexadecimal is case-insensitive).
            hex_str = base64.b16encode(new_email).lower()
            disabled_email = "email.removed+%s@malimedia.be" % hex_str
            log.info('Can change to: "%s".', disabled_email)
            log.info('(Length is: %s)', len(disabled_email))
            cont = input("Continue? (y)es / (n)o : ")
            if cont.lower() != 'y': exit(0)
        else:
            log.info('New emailaddress "%s" not found in BIEDMEE. Good to go...', new_email)
        # Look up in MySQL
        log.info('Looking up old email "%s" in MySQL.', old_email)
        mysql_contact = mysql_get(old_email)
        pprint(mysql_contact)
        # Look up in Mailjet
        log.info('Looking up old email "%s" in Mailjet.', old_email)
        mj_contact = mailjet_get(old_email)
        if not mj_contact:
            log.warn('Old email "%s" not found in Mailjet' % old_email)
        else:
            pprint(mj_contact)
            warn_subscription(mj_contact)
            # Here, we ask for confirmation of the MJ properties
        props = confirm_mj_props(mj_contact)
        pprint(props)
        if clang_id:
            change = input(EM_QUESTION % {'id': clang_id,
                                          'old_email': old_email,
                                          'new_email': new_email})
        else:
            change = input("Since old email didn't exist in BIEDMEE: Continue? (y)es / (n)o : ")
        if change.lower() == 'y':
            # Trigger 'Baseline_Update_Email'-campaign first
            # Add in email and source_ip to props (copy)
            d = deepcopy(props)
            d['email'] = new_email
            try:
                d['source_ip'] = props['optinip']
            except KeyError as e:
                log.error('Property "optinip"/"source_ip" is mandatory.')
                exit(0)
            log.info('Triggering "Baseline_Update_Email" with data: %s', d)
            r = send_to_API(d)
            if clang_id:
                salt_passwd = os.environ['BDM_SALT_PASSWD']
                str_to_hash = salt_passwd + str(clang_id) + salt_passwd + new_email + salt_passwd
                data = {
                    'id'    : clang_id,
                    'email' : new_email,
                    'check' : hashlib.sha1(str_to_hash.encode()).hexdigest()
                }
                log.debug(data)
                if input(UPDATE_QUESTION % {'system': 'Biedmee.be'}).lower() == 'y' or cmd_args.auto:
                    r = requests.post(BDM_URL_CHANGE, data=data)
                    log.debug('Code: %s, Text; %s', r.status_code, r.text)
                    log.info('Biedmee: Changed from "%s" to "%s".', old_email, new_email)
                else:
                    log.info('Skipping....')
            #
            if input(UPDATE_QUESTION % {'system': 'DynamoDB'}).lower() == 'y' or cmd_args.auto:
                update_ddb(new_email)
                log.info('DynamoDB: Added "%s" to table "Emails".', new_email)
            else:
                log.info('Skipping....')
            #
            if input(UPDATE_QUESTION % {'system': 'Mailjet'}).lower() == 'y' or cmd_args.auto:
                update_mailjet(old_email, new_email, props)
            else:
                log.info('Skipping....')
            #
            if input(UPDATE_QUESTION % {'system': 'Odoo'}).lower() == 'y' or cmd_args.auto:
                update_odoo(old_email, new_email)
                log.info('Odoo: partner/contact updated.')
            else:
                log.info('Skipping....')
        else:
            log.info('Exiting...')


if __name__ == '__main__':
    # Parse the command line
    parser = argparse.ArgumentParser(description="""Change emailaddress.
        Provide old and new emailaddres.""")
    parser.add_argument('old_email', type=str, help='Old email')
    parser.add_argument('new_email', type=str, help='New email')
    parser.add_argument('--auto', action='store_true', help='No questions asked')
    #~ group = parser.add_mutually_exclusive_group()
    #~ group.add_argument('--id', dest='clang_id', type=int, help='Clang ID')
    #~ group.add_argument('--uuid', dest='uuid', type=str, help='UUID')
    cmd_args = parser.parse_args()
    log.info('Start')
    old_email   = cmd_args.old_email.strip().lower()
    new_email   = cmd_args.new_email.strip().lower()
    main(old_email, new_email)
    log.info('Finished')


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
