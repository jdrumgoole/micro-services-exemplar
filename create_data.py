import argparse
import random
import string
from enum import Enum

import pymongo
from mimesis import Person, Code, Clothing,Payment
from mimesis.enums import EANFormat
from datetime import datetime


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



class RandomProductGenerator(object):

    PRODUCT_TYPE = {1: 'dress',
                    2: 'scarf',
                    3: 'Jeans',
                    4: 'Hat',
                    5: 'Skirt',
                    6: 'Chinos',
                    7: 'belt',
                    9: 'blouse',
                    10: 'shoes',
                    11: 'boxer shorts',
                    12: 'socks'}

    def __init__(self):

        self._code = Code() # generate product codes
        self._clothing = Clothing() # generate clothing dat
        self._product_map = {}
        self._product_ids = []

    def create_one_product(self, selector=None):

        if selector is None:
            selector = random.randint(1, len(self.PRODUCT_TYPE))
        elif selector not in self.PRODUCT_TYPE.keys():
            raise ValueError( f"Bad Selector {selector}")

        product = {"name": self.PRODUCT_TYPE[selector],
                   "ts": datetime.utcnow(),
                   "product_id": self._code.ean(EANFormat.EAN8),
                   "size": self._clothing.international_size(),
                   "price": random.randint(10, 100),
                   "colour": "#{:06x}".format(random.randint(0, 0xFFFFFF))}

        self._product_ids = product["product_id"]
        return product

    def random_product_id(self):
        selector = random.randint(0, len(self._product_ids) - 1)
        return self._product_ids[selector]

    def generate_random_products(self, count=1000):
        segment = round(count / len(self.PRODUCT_TYPE.keys()))  # generate equal numbers of products for each type
        product_count = 0
        for i,k in enumerate(self.PRODUCT_TYPE.keys()):
            for _ in range(segment):
                product_count = product_count + 1
                print(f"{product_count} creating product {self.PRODUCT_TYPE[k]}")
                self._product_map[i] = self.create_one_product(k)
                yield self._product_map[i]

    @property
    def product_map(self):
        return self._product_map

    def create_product_collection(self, collection, size):
        insert(collection, self.generate_random_products(size))


class RandomUser(object):

    def __init__(self,collection, start_id=None):

        if start_id:
            self._id = start_id
        else:
            self._id = 1

        self._collection = collection
        self._person = Person("en")

    def random_user(self):

        user = {"name": self._person.full_name(),
                "ts" : datetime.utcnow(),
                "username": self._person.email(),
                "password": self._person.password(),
                "user_id": self._id,
                "language": self._person.language(),
                "basket_id": f"basket_{self._id}"}
        self._id += 1
        return user

    def random_users(self, count=1000):
        for i in range(count):
            user=self.random_user()
            name=user["name"]
            print(f"{i+1}. Creating '{name}'")
            yield user


def create_users(users_collection, count, start_id=None):

    user_gen = RandomUser(users_collection, start_id)
    users_collection.create_index("username")
    insert(users_collection, user_gen.random_users(count))


def create_products(collection, count):

    products = RandomProductGenerator()
    products.create_product_collection(collection, count)
    return products


def create_basket_list(users_collection, products):

    for user in users_collection.find():
        print(f"Creating {user['basket_id']}")

        product_list = {}
        no_of_products = random.randint(0, 4)
        for i in range(no_of_products):
            product_sample = random.randint(0, len(products.product_map) -1)
            product = products.product_map[product_sample]
            product_list[product["product_id"]] = products.random_product_id()
            print(f"Adding product'{product['name']}' to {user['basket_id']}")
        else:
            print(f"No products added to basket {user['basket_id']}")

        yield {"ts": datetime.utcnow(),
               "basket_id": user["basket_id"],
               "products": product_list}

def create_baskets(baskets_collection,  items):
    insert(baskets_collection, items)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--host", default="mongodb://localhost:27017", help="mongodb URL for host")
    parser.add_argument("--usercount", type=int, default=100000, help="create this many random users")
    parser.add_argument("--productcount", type=int, default=100000, help="create this many random products")
    parser.add_argument("--drop", default=False, action="store_true", help="Drop the users database")
    parser.add_argument("--startid", type=int, default=1,  help="Start creating userids from this value")
    parser.add_argument("--users", default=False, action="store_true", help="Create users")
    parser.add_argument("--products", default=False, action="store_true", help="create products collection")
    parser.add_argument("--baskets", default=False, action="store_true", help="create baskets collection")
    parser.add_argument("--indexes", default=False, action="store_true", help="make indexes")
    parser.add_argument("--all", action="store_true", default=False, help="make all data")
    args = parser.parse_args()

    client = pymongo.MongoClient(host=args.host)
    db = client["ECOM"]
    users_collection = db["users"]
    baskets_collection = db["baskets"]
    products_collection = db["products"]
    orders_collection = db["orders"]

    users_collection.create_index('ts')
    baskets_collection.create_index('ts')
    products_collection.create_index('ts')
    orders_collection.create_index('ts')

    if args.drop:
        users_collection.drop()
        products_collection.drop()
        baskets_collection.drop()
        orders_collection.drop()

    if args.indexes or args.all:
        users_collection.create_index('ts')
        baskets_collection.create_index('ts')
        products_collection.create_index('ts')
        orders_collection.create_index('ts')

    if args.users or args.all:
        create_users(users_collection, args.usercount, args.startid)

    if args.products or args.all:
        products = create_products(products_collection, args.productcount)

    if args.baskets or args.all:
        create_baskets(baskets_collection, create_basket_list(users_collection, products))



