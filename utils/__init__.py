#
# Copyright 2021- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#
import moment, json
ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
ISO_SHORT_FORMAT = "%Y-%m-%d"

def now():
    return moment.utcnow().strftime(ISO_FORMAT)

def now_short():
    return moment.utcnow().zero.strftime(ISO_SHORT_FORMAT)

def format_date_utc_iso(d):
    return moment.date(d).timezone("UTC").strftime(ISO_FORMAT)

def print_json(data):
    print(json.dumps(data, indent=2))
