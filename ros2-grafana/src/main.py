mport argparse
import subprocess
from datetime import datetime
import re
import threading
import influxdb_client

import hz  # Bu dosya ROS2'dan gelen mesajların hızını hesaplamak için gerekli fonksiyonları içeriyor.


class InfluxDbAccessor:
    """
    InfluxDB veritabanı işlemleri için kullanılan sınıf.
    Bu sınıf, veri yazma ve veritabanı oluşturma işlemlerini gerçekleştirir.
    """
    def __init__(self, url: str, token: str, org: str, bucket_name: str):
        """
        InfluxDB'ye bağlantı kuracak sınıfın kurucu fonksiyonu.
        :param url: InfluxDB'nin URL'si
        :param token: Giriş token'ı
        :param org: InfluxDB organizasyonu
        :param bucket_name: Kullanılacak bucket adı
        """
        self.url = url
        self.token = token
        self.org = org
        self.bucket_name = bucket_name

    def create_bucket(self):
        """
        Belirtilen bucket'ı oluşturur. Eğer varsa, önceki bucket'ı siler.
        """
        with influxdb_client.InfluxDBClient(url=self.url, token=self.token) as client:
            # Buckets API'si ile işlem yapıyoruz
            buckets_api = client.buckets_api()
            buckets = buckets_api.find_buckets().buckets
            # Mevcut bucket'ları kontrol edip, eski bucket'ları siliyoruz
            my_buckets = [bucket for bucket in buckets if bucket.name == self.bucket_name]
            _ = [buckets_api.delete_bucket(my_bucket) for my_bucket in my_buckets]
            _ = buckets_api.create_bucket(bucket_name=self.bucket_name, org=self.org)

    def write_point(self, measurement_datetime, topic_name: str, hz: float):
        """
        InfluxDB'ye bir veri noktası yazar.
        :param measurement_datetime: Ölçüm zamanı
        :param topic_name: Mesajın geldiği ROS topic adı
        :param hz: Mesajın hızı (Hz cinsinden)
        """
        with influxdb_client.InfluxDBClient(url=self.url, token=self.token, org=self.org) as client:
            write_api = client.write_api(write_options=influxdb_client.client.write_api.SYNCHRONOUS)
            # Veriyi oluşturup yazıyoruz
            p = influxdb_client.Point('ros2_topic')\
                .tag('topic_name', topic_name)\
                .time(measurement_datetime)\
                .field('topic_rate_hz', hz)

            write_api.write(bucket=self.bucket_name, record=p)


# Global bir InfluxDbAccessor nesnesi
db: InfluxDbAccessor = None


def update_hz_cb(hz_dict: dict[str, float]):
    """
    Mesaj hızı verilerini işleyen callback fonksiyonu.
    Her bir topic için hızı InfluxDB'ye yazar.
    """
    def run(hz_dict):
        measurement_datetime = int(datetime.now().timestamp() * 1e9)  # Zaman damgasını alıyoruz
        for topic, hz in hz_dict.items():
            # Her topic için veriyi yazıyoruz
            db.write_point(measurement_datetime, topic, hz)
    # Bu işlemi ayrı bir thread'de çalıştırıyoruz
    thread = threading.Thread(target=run, args=(hz_dict,))
    thread.start()


def subscribe_topic_hz(topic_list: list[str], window_size: int):
    """
    Verilen topic listesi ile ROS2 topic'lerinin hızını izler.
    """
    hz_verb = hz.HzVerb()  # Hz hesaplama fonksiyonları
    parser = argparse.ArgumentParser()
    hz_verb.add_arguments(parser, 'ros2_monitor_grafana')
    args = parser.parse_args('')  # Parametreleri alıyoruz
    args.topic_list = topic_list
    args.window_size = window_size
    hz.main(args, update_hz_cb)  # Mesaj hızı hesaplama işlemi başlatılıyor


def make_topic_list(ignore_regexp: str, target_regexp: str) -> list[str]:
    """
    ROS2 topic listesini alır ve belirli regex'lere göre filtreler.
    """
    topic_list = subprocess.run(['ros2', 'topic', 'list'],
                                capture_output=True,
                                text=True)
    topic_list = topic_list.stdout.splitlines()
    # target_regexp ile eşleşen, ignore_regexp ile eşleşmeyen topic'leri seçiyoruz
    topic_list = [topic for topic in topic_list if \
        re.search(target_regexp, topic) and not re.search(ignore_regexp, topic)]

    len_topic_list = len(topic_list)
    print(f'İzlenen topic sayısı: {len_topic_list}')
    if len_topic_list > 50:
        print('Uyarı: Çok fazla topic izleniyor, sonuçlar doğruluğu etkileyebilir. ' +
              'Lütfen ignore_regexp ve target_regexp parametrelerini kullanın.')

    return topic_list


def parse_args():
    """
    Komut satırı argümanlarını çözümler.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--ignore_regexp', type=str, default='(parameter_events|rosout|debug|tf)')
    parser.add_argument('--target_regexp', type=str, default='.*')
    parser.add_argument('--window_size', type=int, default=10)
    parser.add_argument('--token', type=str, default='my-super-secret-auth-token',
                        help='InfluxDB token')
    parser.add_argument('--org', type=str, default='my-org',
                        help='InfluxDB organization')
    parser.add_argument('--url', type=str, default='http://localhost:8086',
                        help='InfluxDB URL')
    parser.add_argument('--bucket_name', type=str, default='my-bucket',
                        help='InfluxDB bucket name')
    args = parser.parse_args()
    return args


def main():
    """
    Programın ana fonksiyonu. 
    Argümanları okur, InfluxDB bağlantısı kurar ve topic hızlarını izlemeye başlar.
    """
    args = parse_args()  # Argümanları alıyoruz

    global db
    db = InfluxDbAccessor(args.url, args.token, args.org, args.bucket_name)
    db.create_bucket()  # Veritabanını oluşturuyoruz

    # Topic listelerini oluşturuyoruz
    topic_list = make_topic_list(args.ignore_regexp, args.target_regexp)
    subscribe_topic_hz(topic_list, args.window_size)  # Topic'leri izlemeye başlıyoruz


if __name__ == '__main__':
    main()  # Ana fonksiyonu çalıştırıyoruz
