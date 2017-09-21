# invoke_mj_to_s3.py

AWS EC2-function to:

 - invoke the mj_to_s3-fn's

This process will invoke 1 λ for every MAX_LIMIT (1000) resources
In such a way that no more than payload['MaxCallsPerMin'] (btween 300 and 600)
would reach Mailjet.

If we get the resource 'contact', 'contactdata' and 'listsrecipient'
a total of about 5.1 milj resources should be fetched and thus
5.1k calls be made. A rato of 300 calls/minute this would take about
17 minutes.

This script can randomize the time between in invocations.

Start the script with:

```shell
$ ./invoke_mj_to_s3.py -r contact -m 8 --auto --uniform-random > contact.log &
```
Ask for help with:

```shell
$ ./invoke_mj_to_s3.py -h
```
