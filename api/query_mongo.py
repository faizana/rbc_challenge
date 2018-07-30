from pymongo import MongoClient
import json
import re
from bson.json_util import dumps




client = MongoClient()
db = client.etl
cus_trans_data = db.cus_trans_data


def prepare_mongo_query(income_range, city, spending, month):
    """
    Constructs query for mongo db
    :param income_range: The income range of customers
    :param city: The city or area
    :param spending: The amount of spending the customer did
    :param month: The month of the year from 1-12
    :return:
    """
    city = re.compile(".*{}.*".format(city), re.IGNORECASE)
    pipeline = [{
        "$match": {
            "$and": [
                {"income":
                    {
                        "$gt": income_range['min'],
                        "$lt": income_range['max']
                    }
                }, {"month": month}, {"city": city}]
        }
    }, {
        "$group": {
            "_id": "$account_id",
            "spending": {
                "$sum": "$transaction_amount"
            }
        }
    },
        {
            "$match": {
                "spending": {
                    "$gt": spending
                }
            }
        }
    ]

    res = cus_trans_data.aggregate(pipeline)
    return json.loads(dumps(res))




