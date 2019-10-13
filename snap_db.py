"""
Simple program to watch a transactions collection. Defaults to the port 7100 and replica set 27100
"""
import pymongo
from argparse import ArgumentParser
from datetime import datetime
import pprint
import sys

if __name__ == "__main__":

    parser = ArgumentParser()

    parser.add_argument("--host", default="mongodb://localhost:27017",
                        help="mongodb URI for connecting to server [default: %(default)s]")
    parser.add_argument("--watch", default="ECOM.baskets", help="Watch <database.collection> [default: %(default)s]")
    parser.add_argument("--snap", default="ECOM.baskets_snap", help="Store to <database.collection> [default: %(default)s]")

    args = parser.parse_args()

    client = pymongo.MongoClient(host=args.host)

    watch_collection = None
    snap_collection = None

    if args.snap:
        (snap_db_name, sep, snap_collection_name) = args.snap.partition(".")
        snap_db = client[snap_db_name]
        snap_collection = snap_db[snap_collection_name]
        print(f"output to {args.snap}")
    if args.watch:
        (watch_db_name, sep, watch_collection_name) = args.watch.partition(".")
        watch_db = client[watch_db_name]
        watch_collection = watch_db[watch_collection_name]
        print(f"Watching:{args.watch}\n")

    try:
        while True:
            print("Creating new watch cursor")
            watch_cursor = watch_collection.watch()

            for d in watch_cursor:
                if d["operationType"] == "invalidate":

                    print("Watch cursor invalidated (deleted collection?)")
                    #pprint.pprint(d)
                    print("Closing cursor")
                    watch_cursor.close()
                    break
                elif snap_collection:
                    if len(d["fullDocument"]["products"]) > 0: # we sold something
                        del d["fullDocument"]["_id"]
                        print("local time   : {}".format(datetime.utcnow()))
                        print("cluster time : {}".format(d["clusterTime"].as_datetime()))
                        print("collection   : {}.{}".format(d["ns"]["db"], d["ns"]["coll"]))
                        pprint.pprint(d["fullDocument"])
                        snap_collection.insert_one({"basket_id": d["fullDocument"]["basket_id"], "doc": d["fullDocument"]})

    except KeyboardInterrupt:
        print("Closing watch cursor")
        watch_cursor.close()
        print("exiting...")
