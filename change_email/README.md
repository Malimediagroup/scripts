# change_email.py

Basic interactive cli script as a workaround to change a user's emailaddress
in the different systems.
Single-file command line script to change a user's emailaddress in the
various places where it is present.

Various keys need to be in the environment:

```shell
$ source env.secrets
```

AWS credentials (for DynamoDB) in the usual place.

Where is a user's emailaddress present?
- DDB:
    - Emails    (add ipv change)
    - Contacts  (change)
- MySQL:
    - Contacts  (change + split/canonicalize)
- MailJet:
    - Check All (Bdm) Accounts: Main and Transactional (Extern)
        -> unsubscribe/remove from all lists + subscribe new address with
           all contactdata (uuid, fname, lname, ... seg_num, block)
- Bdm:
    - change_script: os.environ['BDM_URL_CHANGE']
- Odoo:
    - res.partner
- Zendesk:
    - TBI

Start the script with:

```shell
$ ./change_email.py <old_email> <new_email>
```
Ask for help with:

```shell
$ ./change_email.py -h
```
