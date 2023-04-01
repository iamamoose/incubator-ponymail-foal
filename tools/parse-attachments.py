#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
    parse-attachments.py: store a text version of all attachments of mails
    uses Apache Tika for conversions.  Read the python tika docs before using as
    it will reach out to apache.org and download Tika on first run

    Also this adds a field to the ponymail-mbox index. So first time you run this
    you need to uncomment the await es.indices... line to add the new field

    Be sure to always run your query with --test first, to see which documents would be affected!
"""

import elasticsearch.exceptions
import sys
import asyncio
import argparse
import time
import re
import warnings
import base64
import tika
from elasticsearch.helpers import async_scan

tika.initVM();
from tika import parser

if not __package__:
    from plugins import ponymailconfig
    from plugins.elastic import Elastic
else:
    from .plugins import ponymailconfig
    from .plugins.elastic import Elastic


def gen_args() -> argparse.Namespace:
    """Generate/parse CLI arguments"""
    parser = argparse.ArgumentParser(description="Command line options.")
    parser.add_argument(
        "--search",
        dest="search",
        nargs=1,
        help="""Search parameters (ElasticSearch query string) to narrow down what to edit (for instance: 'list_raw:"<dev.maven.apache.org>"')""",
        default="*",
    ),
    parser.add_argument(
        "--test",
        dest="test",
        action="store_true",
        help="Test mode, only scan database and report, but do not make any changes to it.",
    )
    args = parser.parse_args()
    return args


async def main():
    start_time = time.time()
    args = gen_args()
    config = ponymailconfig.PonymailConfig()
    es = Elastic(is_async=True)
    warnings.filterwarnings("ignore", category=elasticsearch.exceptions.ElasticsearchWarning)
    docs_changed = 0

#   await es.indices.put_mapping(index=es.db_mbox, body={"properties":{"attachmenttext": {"type":"text"}}})

    async for doc in async_scan(client=es.es, q=args.search, index=es.db_mbox):
        source = doc["_source"]
        if ('attachmenttext' not in source and source['attachments']):
            attachmenttext = []
            print(f"""found: {doc['_id']} {source['list_raw']}: {source['subject']}""")
            for attach in source['attachments']:
                x = await es.es.get(index=es.db_attachment, id=attach['hash'])
                if x:
                    blob = base64.decodebytes(x["_source"].get("source").encode("utf-8"))                    
                    parsed = parser.from_buffer(blob)
                    if parsed and parsed["content"]:
                        print ("Attachment parsed: ",attach['content_type'],attach['filename'])
                        docs_changed += 1
                        attachmenttext.append(["content_type: "+attach['content_type'],"filename: "+attach['filename'],parsed["content"]])
            if not attachmenttext:
                attachmenttext = []  # put one in anyway to show there was nothing parsed                
            if not args.test:
                await es.es.update(
                    index=es.db_mbox,
                    id=doc["_id"],
                    body={
                        "doc": {
                            "attachmenttext": attachmenttext
                        }
                    },
                )
            else:
                print (attachmenttext)

    stop_time = time.time()
    time_taken = int(stop_time - start_time)
    print(f"Handled {docs_changed} attachment(s) in {time_taken} second(s).")
    await es.es.close()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
