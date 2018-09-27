import argparse
import random
import string
import pymongo
from mimesis import Person, Code, ClothingSize
from mimesis.enums import EANFormat


def insert(collection, items):
    insert_list = []
    for i, item in enumerate(items,1):
        print(f"{i}. {item}")
        insert_list.append(item)
        if i % 1000 == 0:
            collection.insert_many(insert_list)
            insert_list = []
    if len(insert_list) > 0:
        users_collection.insert_many(insert_list)

    return i


class RandomProduct(object):

    def __init__(self):


        self._code = Code()
        self._clothing_size = ClothingSize()

        self._products = {1: 'dress',
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

    def create_product(self, selector=None):

        if selector is None:
            selector = random.randint(1,12)
        elif selector not in self._products.keys():
            raise ValueError( f"Bad Selector {selector}")

        product = {"name": self._products[selector],
                   "EAN": self._code.ean(EANFormat.EAN8),
                   "size": self._clothing_size.international_size(),
                   "colour": "#{:06x}".format(random.randint(0, 0xFFFFFF))}

        return product

    def create_products(self, count=1000):
        segment = round(count / len(self._products.keys()))
        for i in self._products.keys():
            for _ in range(segment):
                yield self.create_product(i)

    def create_product_collection(self, collection, size):
        insert(collection, self.create_products(size))


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
                "username": self._person.email(),
                "password": self._person.password(),
                "userid": self._id,
                "language": self._person.language()}
        self._id += 1
        return user

    def random_users(self, count=1000):
        for _ in range(count):
            yield self.random_user()


def create_users(collection, start_id=None):


    user_gen = RandomUser(users_collection, start_id)
    users_collection.create_index("username")

    insert(users_collection, user_gen.random_users(args.usercount))

def create_products(collection, count):

    products = RandomProduct()
    insert(collection, products.create_products(count))


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--host", default="mongodb://localhost:27017", help="mongodb URL for host")
    parser.add_argument("--usercount", type=int, default=10000, help="create this many random users")
    parser.add_argument("--drop", default=False, action="store_true", help="Drop the users database")
    parser.add_argument("--startid", type=int, default=1,  help="Start creating userids from this value")
    parser.add_argument("--users", default=False, action="store_true", help="Create users")
    parser.add_argument("--products", default=False, action="store_true", help="create products")
    args = parser.parse_args()

    client = pymongo.MongoClient(host=args.host)
    db = client["ECOM"]
    users_collection = db["users"]
    baskets_collection = db["baskets"]
    products_collection = db["products"]

    if args.drop:
        users_collection.drop()
        products_collection.drop()

    if args.users:
        create_users(users_collection, args.startid)

    if args.products:
        create_products(products_collection,1000)





