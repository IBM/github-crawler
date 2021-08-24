#
# Copyright 2021- IBM Inc. All rights reserved
# SPDX-License-Identifier: Apache2.0
#
import os, requests
from . import now
from cloudant.client import Cloudant
from cloudant.adapters import Replay429Adapter
from dotenv import load_dotenv
load_dotenv()

cloudant_client = Cloudant.iam(None, os.getenv("CLOUDANT_API_KEY"), connect=True,
    url=os.getenv("CLOUDANT_URL"),
    adapter=Replay429Adapter(retries=10, initialBackoff=0.01))
cloudant_client.session()

db_name = os.getenv("CLOUDANT_DB")

try:
    cloudant_db = cloudant_client[db_name]
except:
    cloudant_db = cloudant_client.create_database(db_name)
    cloudant_db.create_query_index(fields=[ "_id", "type", "email", "topics", "stars", "crawled_updated_at" ])

if cloudant_db.exists():
    print('SUCCESS connecting to Cloudant db', db_name)

    if "_design/types" not in cloudant_db:
        ddoc = cloudant_db.create_document({ "_id": "_design/types"} )
        ddoc.add_view("types", "function (doc) {\n  emit(doc.type, 1);\n}", "_count")
        ddoc.save()

def save_doc(id, content, overwrite=True, timestamp=True):
    if "contributions" in content and "type" in content and content["type"] == "User":
        del content["contributions"]
    if timestamp:
        content["crawled_updated_at"] = now()
    try:
        if id in cloudant_db:
            my_doc = cloudant_db[id]
            if overwrite:
                for k in content.keys():
                    my_doc[k] = content[k]
                
                try:
                    my_doc.save()
                    if my_doc.exists():
                        print('UPDATED!!', my_doc.get("type", "No type"), my_doc["_id"])
                except requests.exceptions.HTTPError as e:
                    print(str(e))
                    if "412 Client Error" in str(e):
                        pass
                    else:
                        raise e
                
            else:
                print('ignored!!', my_doc.get("type", "No type"), my_doc["_id"])
        else:
            content["_id"] = id
            if timestamp:
                content["crawled_created_at"] = now()
            my_doc = cloudant_db.create_document(content)
            # Check that the document exists in the database
            if my_doc.exists():
                print('CREATED!!', my_doc.get("type", "No type"), my_doc["_id"])
        return my_doc
    except Exception as e:
        print(str(e), id, content)
        pass

