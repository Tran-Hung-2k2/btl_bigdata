from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
from dotenv import load_dotenv
import config as config
from io_cluster import *
from query import Query


# Load environment variables from .env
load_dotenv()

schema = StructType([
    StructField("views", IntegerType(), True),
    StructField("num_answer", IntegerType(), True),
    StructField("votes", IntegerType(), True),
    StructField("solved", BooleanType(), True),
    StructField("title", StringType(), True),
    StructField("content", StringType(), True),
    StructField("time", TimestampType(), True),
    StructField("category", ArrayType(StringType()), True)
])

if __name__ == "__main__":
    # Declare config variables
    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_key = os.getenv("AWS_SECRET_KEY")
    s3_bucket = os.getenv("S3_BUCKET")
    s3_path = os.getenv("S3_PATH")
    elasticsearch_host = os.environ.get("ELASTICSEARCH_HOST")
    elasticsearch_port = os.environ.get("ELASTICSEARCH_PORT")
    s3_full_path = f"s3a://{s3_bucket}/{s3_path}"
    app_name ="Big data project"

    # create spark app
    app_config = config.Config(elasticsearch_host=elasticsearch_host,
                               elasticsearch_port=elasticsearch_port,
                               elasticsearch_nodes_wan_only="true",
                               s3_access_key=aws_access_key,
                               s3_secret_key=aws_secret_key,
                               )
    spark = app_config.initialize_spark_session(app_name)
    original_df = read_dataframe_from_s3(spark, s3_full_path)

    # Tổng số lượng câu hỏi
    total_questions = Query.get_total_questions(df)

    # Số lượng câu hỏi đã có giải đáp và chưa có giải đáp
    solved_questions = Query.get_solved_questions(df)
    unsolved_questions = Query.get_unsolved_questions(df)

    # Trung bình lượt views của tất cả câu hỏi
    average_views = Query.get_average_views(df)

    # Câu hỏi trong khoảng thời gian
    questions_within_time_df = Query.get_questions_within_time(df, "2023-01-01", "2023-12-31")

    # Tỉ lệ giải đáp câu hỏi trong khoảng thời gian
    solved_questions_within_time_ratio = Query.get_solved_questions_within_time_ratio(df, "2023-01-01", "2023-12-31")

    # Đếm số từ trong mỗi câu hỏi
    word_count_df = Query.get_counted_word_in_questions(original_df)
    word_count_df.cache()

    # Số từ trung bình
    averages_word_counts = word_count_df.agg(avg("word_count")).collect()[0][0]

    # Lưu vào elasticsearch
    df_save_to_es = (original_df, word_count_df)
    df_es_indices = ("original", "word_count")

    save_dataframes_to_elasticsearch(df_save_to_es, df_save_to_es, spark.get_elasticsearch_conf())

    # Stop
    spark.stop()

