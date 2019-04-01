import argparse
import pymongo
from datetime import datetime
import random
import time
from transaction_retry import Transaction_Functor, run_transaction_with_retry

def get_latest(collection, query, bale=True):
    cursor = collection.find(query).sort("ts", pymongo.DESCENDING).limit(1)
    try:
        return cursor.next()
    except StopIteration:
        if bale:
            raise ValueError(query)
        else:
            return None

class BasketManager(object):

    def __init__(self, baskets_collection, products_collection, orders_collection):

        self._baskets_collection = baskets_collection
        self._products_collection = products_collection
        self._orders_collection = orders_collection

    def get_latest_basket(self, basket_id):
        return get_latest(self._baskets_collection, {"basket_id": basket_id})

    def get_products(self, basket_id):
        basket = self.get_latest_basket(basket_id)
        return basket["products"]

    def get_product(self, basket_id, product_id):
        products = self.get_products(basket_id)
        return products[product_id]

    def adjust_product(self, basket_id, product_id, adjust=1):

        basket=self.get_latest_basket(basket_id)
        products=basket["products"]

        if product_id in products:
            products[product_id] = products[product_id] + adjust
            if products[product_id] < 0:
                products[product_id] = 0
        elif adjust > 0:
            products[product_id] = adjust

        del basket["_id"]
        basket["ts"] = datetime.utcnow()
        basket["products"] = products
        #print("Adjusted basket {}".format(basket))
        self._baskets_collection.insert_one(basket)

    def adjust_basket(self, basket_id, product_id, func_select=None):

        if func_select:
            flip = func_select
        else:
            flip = random.randint(1, 3)

        if flip == 1:
            increase=random.randint(1,5)
            print(f"Increasing product {product_id} by {increase} units")
            self.adjust_product( basket_id, product_id, increase)
        elif flip == 2:
            decrease = -random.randint(1,2)
            self.adjust_product(basket_id, product_id, decrease)
            print(f"Decreasing product {product_id} by {decrease} units")
        elif flip == 3:
            print(f"Buying basket {basket_id}")
            self.buy(basket_id)

    def txn_write(self, basket, total_price, session=None):
        one_result = self._orders_collection.insert_one({"ts": datetime.utcnow(),
                                                         "basket_id": basket["basket_id"],
                                                         "total_price": total_price,
                                                         "reference": basket["_id"]},
                                                         session=session)
        print("Created order for {}".format(basket["basket_id"]))
        basket["order_id"] = one_result.inserted_id
        del basket["_id"]
        basket = {"ts": datetime.utcnow(),
                  "basket_id": basket["basket_id"],
                  "products": {}}
        self._baskets_collection.insert_one(basket, session=session)

    def buy(self, basket_id):
        basket = self.get_latest_basket(basket_id)
        total_price = 0
        #print(basket)
        for product_id, item_count in basket["products"].items():
            product = get_latest(self._products_collection, { "_id":product_id})
            total_price = total_price + product["price"]

        if total_price == 0:
            print("No order - no goods in basket")
            return

        txn_functor = Transaction_Functor(self.txn_write, basket, total_price)

        with client.start_session() as session:
            run_transaction_with_retry(txn_functor, session)

    # def basket_snapshot(self, basket_id):
    #
    #
    #     self._collection.aggregate([{"$match" : {"basket_id" : basket_id}},
    #                                 {"$group": { "_id": "$basket_id",
    #                                               "products" : { "$addToSet" : "$product"}}},
    #                                 {"$group": {"_id": "$basket_id",
    #                                              "products": {"$addToSet": "$product"}}},
    #                                 {"$out" : "added_products"}])


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--host", default="mongodb://localhost:27017", help="mongodb URL for host")
    parser.add_argument("--delay", type=float, default=0.5, help="delay between basket adjustment")
    parser.add_argument("--selector", type=int, choices=[1,2,3], default=None, help="1=Increase, 2=Decrease, 3=Buy (default all)")

    args = parser.parse_args()

    client = pymongo.MongoClient(host=args.host)
    db = client["ECOM"]
    users_collection = db["users"]
    baskets_collection = db["baskets"]
    products_collection = db["products"]
    orders_collection = db["orders"]
    orders_collection.create_index("basket_id")

    basket_mgr = BasketManager(baskets_collection, products_collection, orders_collection)
    while True:
        products = products_collection.aggregate([{"$sample": {"size":10}}])
        for p in products:
            users = users_collection.aggregate([{"$sample": {"size":10}}])
            for u in users:
                print( "\nadjusting {} for '{}'".format( u["basket_id"], u["name"]))
                basket_mgr.adjust_basket( u["basket_id"], p["_id"], args.selector)
                time.sleep(args.delay)

