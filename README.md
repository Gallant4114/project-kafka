# project-kafka

| Nama              | NRP        |
| ----------------- | ---------- |
| Irfan Qobus Salim | 5027221058 |
| Ricko Mianto      | 5027231031 |
| Gallant Damas     | 5027231037 |

## Tentang Dataset

Dataset ini berisi lebih dari 1 juta transaksi kartu kredit yang disimulasikan, dirancang untuk mendukung pembelajaran dan eksperimen dalam deteksi penipuan (fraud detection). Dataset ini diunggah oleh pengguna Hugging Face `dazzle-nu` dan tersedia dalam format CSV dengan ukuran sekitar 267 MB. Meskipun dataset ini cukup besar, distribusi label fraud dan non fraud sangat tidak imbang, dengan perbandingan **99.4%** untuk non fraud dan **0.6%** untuk fraud. Penjelasan lengkap tentang dataset terdapat pada https://huggingface.co/datasets/dazzle-nu/CIS435-CreditCardFraudDetection

## Kafka Producer

Producer mengirim data tiap baris dari file csv secara terus menerus dengan delay antara 0.01 detik hingga 0.1 detik

## Kafka Consumer

Consumer menerima data tiap baris dari producer dan menjadikannya batch untuk tiap 5500 data. Berdasarkan perhitungan, untuk membuat 1 batch berisi 5500 baris diperlukan waktu kurang lebih 5 menit. Jika ada total 3 batch atau 16500 baris maka waktu yang diperlukan adalah 15 menit.

## Training Model

Setelah consumer mengolah data menjadi 3 batch, tahap selanjutnya adalah pelatihan model berdasarkan dataset tiap batch. Ada 3 model yang dibuat berdasarkan batch dataset yang berbeda. Nantinya ketiga model tersebut diexport agar bisa digunakan melalui API pada backend. Proses dari training model adalah sebagai berikut:

- Pre-processing Data:

  - Membaca data transaksi dari file JSON.
  - Memilih fitur-fitur penting seperti **amt, lat, long, city_pop, unix_time, merch_lat, dan merch_long.**
  - Menggunakan VectorAssembler untuk menggabungkan fitur-fitur tersebut menjadi satu vektor fitur.

- Training Model:

  - Menggunakan algoritma klasifikasi seperti Logistic Regression untuk melatih model pada data yang telah diproses.
  - Membagi data menjadi beberapa batch untuk pelatihan model secara terpisah.

- Evaluasi Model:

  - Mengevaluasi kinerja model menggunakan metrik seperti akurasi dan presisi
  - Menyimpan hasil evaluasi untuk analisis lebih lanjut.

- Penyimpanan Model:
  - Menyimpan model yang telah dilatih ke dalam direktori `models/` dengan format **fraud_model_batch_{id}**.
  - Model disimpan sebagai **PipelineModel** untuk memudahkan penggunaan kembali dalam sistem backend.

## Backend API

Backend API dibuat menggunakan **Flask** dan **PySpark** untuk memprediksi apakah suatu transaksi kartu kredit merupakan tindakan penipuan (fraud) atau bukan. Sistem ini menggunakan model Machine Learning berbasis Spark MLlib yang telah dilatih per batch dan diekspor ke dalam pipeline terpisah.

Ada 3 endpoint yang kami buat, yaitu:

- `/predict_all_models` untuk memprediksi fraud dengan 3 model yang sudah ditraining.
- `/fraud_history/<merchant>` untuk menampilkan riwayat transaksi fraud yang terjadi pada merchant tertentu.
- `/fraud_stats` untuk mengembalikan statistik agregat dari jumlah transaksi fraud dan non-fraud yang terdata.

**Dokumentasi**

- POST `/predict_all_models`
![image](https://github.com/user-attachments/assets/4ef7b3b0-29e1-45bb-bb9e-084d63031961)

- GET `/fraud_history/<merchant>`
![image](https://github.com/user-attachments/assets/63c54878-644a-4383-9bc3-8e2471858242)

- GET `/fraud_stats`
![image](https://github.com/user-attachments/assets/222788a0-658c-4fa9-85c3-148f00ada356)
