from flask import Flask, jsonify, request
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import Row
import json
import glob
import os

app = Flask(__name__)
spark = SparkSession.builder.appName("FraudPredictionAPI").getOrCreate()

MODEL_DIR = "/home/irfanqs/project-kafka/models"

# Daftar list fitur yang digunakan untuk prediksi
FEATURE_LIST = [
    "amt", "lat", "long", "city_pop", "unix_time", "merch_lat", "merch_long"
]

def load_model_for_batch(batch_id):
    path = os.path.join(MODEL_DIR, f"fraud_model_batch_{batch_id}")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Model path not found: {path}")
    return PipelineModel.load(path)

def load_data_from_json_files():
    data = []
    json_files = sorted(glob.glob("batches/batch_*.json"))[:3]
    for file_name in json_files:
        with open(file_name, 'r') as file:
            try:
                content = json.load(file)
                if isinstance(content, list):
                    data.extend(content)
                else:
                    print(f"File {file_name} tidak berformat list")
            except json.JSONDecodeError:
                print(f"File {file_name} tidak bisa dibaca sebagai JSON")
    return data

data_transaksi_fraud = load_data_from_json_files()

# Endpoint untuk memprediksi
@app.route('/predict_all_models', methods=['POST'])
def predict_all_models():
    input_data = request.get_json()
    predictions = {}

    for batch_id in range(3):
        try:
            model = load_model_for_batch(batch_id)

            # Konversi input ke DataFrame langsung
            data_dict = {feature: float(input_data[feature]) for feature in FEATURE_LIST}
            df = spark.createDataFrame([data_dict])

            # Jangan buat kolom 'features' lagi — biarkan pipeline model yang handle
            result = model.transform(df).collect()[0]

            predictions[f"batch_{batch_id}"] = {
                "prediction": int(result.prediction),
                "probability": float(result.probability[1])
            }

        except Exception as e:
            predictions[f"batch_{batch_id}"] = {"error": str(e)}

    return jsonify(predictions)

# Endpoint Fraud History dari suatu merchant
@app.route('/fraud_history/<merchant>', methods=['GET'])
def get_fraud_history_by_merchant(merchant):
    merchant = merchant.lower()

    fraud_transactions = [
        record for record in data_transaksi_fraud
        if record.get("is_fraud") == "1" and merchant in record.get("merchant", "").lower()
    ]

    return jsonify({
        "merchant": merchant,
        "fraud_count": len(fraud_transactions),
        "fraud_transactions": fraud_transactions
    })

# Endpoint Fraud Stats
@app.route('/fraud_stats', methods=['GET'])
def get_fraud_stats():
    fraud_count = 0
    not_fraud_count = 0

    for record in data_transaksi_fraud:
        if record.get("is_fraud") == "1":
            fraud_count += 1
        elif record.get("is_fraud") == "0":
            not_fraud_count += 1

    stats = {
        "fraud_transactions": fraud_count,
        "non_fraud_transactions": not_fraud_count
    }
    return jsonify(stats)

if __name__ == '__main__':
    app.run(debug=True)
