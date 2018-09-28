
import pymongo

def get_latest(collection, query, bale=True):
    cursor = collection.find(query).sort("ts", pymongo.DESCENDING).limit(1)
    try:
        return cursor.next()
    except StopIteration:
        if bale:
            raise ValueError(query)
        else:
            return None


if __name__ == "__main__":

    client = pymongo.MongoClient()
    db=client["ECOM"]
    col=db["baskets"]

    for i in range(1,10001):
        if get_latest(col, { "basket_id" : f"basket_{i}"}):
            print(f"fuck{i}")
