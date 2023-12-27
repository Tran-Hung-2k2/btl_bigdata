from pyspark.sql import SparkSession

class Config:
    def __init__(self,
                 elasticsearch_host,
                 elasticsearch_port,
                 elasticsearch_nodes_wan_only,
                 s3_access_key,
                 s3_secret_key,
                 ):
        self.elasticsearch_conf = {
            'es.nodes': elasticsearch_host,
            'es.port': elasticsearch_port,
            "es.nodes.wan.only": elasticsearch_nodes_wan_only,
            "es.net.http.auth.user": "",  # Đặt tên người dùng trống nếu không có xác thực
            "es.net.http.auth.pass": "",  # Đặt mật khẩu trống nếu không có xác thực
        }
        self.s3_conf = {
            'spark.hadoop.fs.s3a.access.key': s3_access_key,
            'spark.hadoop.fs.s3a.secret.key': s3_secret_key,
        }
        self.spark_app = None

    def get_elasticsearch_conf(self):
        return self.elasticsearch_conf

    def get_s3_conf(self):
        return self.s3_conf

    def initialize_spark_session(self, appName):
        if self.spark_app is None:
            self.spark_app = (SparkSession
                              .builder.master("spark://spark-master:7077")
                              .appName(appName)
                              .config("spark.jars", "path/to/elasticsearch-hadoop-7.15.1.jar")
                              .config("spark.driver.extraClassPath", "path/to/elasticsearch-hadoop-7.15.1.jar")
                              .config("spark.es.nodes", self.elasticsearch_conf["es.nodes"])
                              .config("spark.es.port", self.elasticsearch_conf["es.port"])
                              .config("spark.es.nodes.wan.only", self.elasticsearch_conf["es.nodes.wan.only"])
                              .config("spark.es.net.http.auth.user", self.elasticsearch_conf["es.net.http.auth.user"])
                              .config("spark.es.net.http.auth.pass", self.elasticsearch_conf["es.net.http.auth.pass"])
                              .config("spark.hadoop.fs.s3a.access.key", self.s3_conf["spark.hadoop.fs.s3a.access.key"])
                              .config("spark.hadoop.fs.s3a.secret.key", self.s3_conf["spark.hadoop.fs.s3a.secret.key"])
                              .getOrCreate())
        return self.spark_app