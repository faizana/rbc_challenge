from flask import Flask, jsonify, request
from query_mongo import prepare_mongo_query


# *The* app object
app = Flask(__name__)


@app.route("/api/v1/extract_users", methods=['POST'])
def extract_users():
    post_data = request.get_json()
    # REQUIRED: Validate the post_data arguments.

    income_range = post_data['income_range']
    city = post_data['city']
    spending = post_data['spending']
    month = post_data['month']
    query_result = prepare_mongo_query(income_range, city, spending, month)

    return jsonify(query_result)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8000)
