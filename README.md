# project-kafka

| Nama              | NRP        |
| ----------------- | ---------- |
| Irfan Qobus Salim | 5027221058 |
| Ricko Mianto      | 5027231031   |
| Gallant Damas     | 5027231037   |

## Kafka Producer

Producer mengirim data tiap baris dari file csv secara terus menerus dengan delay antara 0.01 detik hingga 0.1 detik

## Kafka Consumer

Consumer menerima data tiap baris dari producer dan menjadikannya batch untuk tiap 5500 data. Berdasarkan perhitungan, untuk membuat 1 batch berisi 5500 baris diperlukan waktu kurang lebih 5 menit. Jika ada total 3 batch atau 16500 baris maka waktu yang diperlukan adalah 15 menit.
