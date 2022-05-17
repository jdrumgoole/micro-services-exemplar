import argparse
import pprint

import pymongo
from mimesis import Datetime, Finance, Code


def insert(collection, items, batch_size=1000):
    insert_list = []
    i=0
    for i, item in enumerate(items, 1):
        #print(f"Inserting {item}")
        insert_list.append(item)
        if i % batch_size == 0:
            collection.insert_many(insert_list)
            insert_list = []
    if len(insert_list) > 0:
        collection.insert_many(insert_list)
    return i


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--host", default="mongodb://localhost:27017", help="mongodb URL for host")
    parser.add_argument("--database", default="PRODUCTS", help="default database [PRODUCTS]")
    parser.add_argument("--collection", default="plants", help="default collection [plants]")
    parser.add_argument("--names", default="names.txt")
    parser.add_argument("--families", default="families.txt")
    parser.add_argument("--drop", default=False, action="store_true", help="Drop the users database")

    args = parser.parse_args()

    d = Datetime()
    f = Finance()
    e = Code()
    client = pymongo.MongoClient(host=args.host)
    db = client[args.database]
    products_collection = db[args.collection]
    insert_list = []
    with open(args.names, "r") as names_file:
        with open(args.families, "r") as families_file:
            i = 0
            for name, family in zip(names_file, families_file):
                i = i + 1
                name = name.strip()
                family = family.strip()
                if family == "":
                    continue
                elif family == name:
                    continue
                else:
                    print(f"{i}. {name}:{family}")
                    doc = {"name": name,
                           "family": family,
                           "price": f.price(5, 80),
                           "release_date": d.datetime(start=2021),
                           "product_number": e.ean()}
                    pprint.pprint(doc)
                    insert_list.append(doc)
                    if len(insert_list) > 999:
                        products_collection.insert_many(insert_list)
                        insert_list = []
            if len(insert_list) > 0:
                products_collection.insert_many(insert_list)



