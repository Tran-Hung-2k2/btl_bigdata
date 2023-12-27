from pyspark.sql.functions import *
from pyspark.sql.types import *
from helper import *

class Query:
    @staticmethod
    @udf(returnType=IntegerType())
    def get_total_questions(df): 
        return df.count()
    
    @staticmethod
    @udf(returnType=IntegerType())
    def get_solved_questions(df):
        return df.filter(col("solved") == True).count()
    
    @staticmethod
    @udf(returnType=IntegerType())
    def get_unsolved_questions(df):
        return df.filter(col("solved") == False).count()
    
    @staticmethod
    @udf(returnType=FloatType())
    def get_average_views(df):
        return df.agg(avg("views")).collect()[0][0]
    
    @staticmethod
    def get_questions_within_time(df, start_date, end_date):
        return df.filter((col("time") >= start_date) & (col("time") <= end_date))
    
    @staticmethod
    def get_solved_questions_within_time(df, start_date, end_date):
        return df.filter((col("time") >= start_date) & (col("time") <= end_date)).filter(col("solved") == True).count()
    
    @staticmethod
    @udf(returnType=FloatType())
    def get_solved_questions_within_time_ratio(df, start_date, end_date):
        return Query.get_solved_questions_within_time(df, start_date=start_date, end_date=end_date).count() / Query.get_questions_within_time(df, start_date=start_date, end_date=end_date).count()
    
    @staticmethod
    def get_counted_word_in_questions(df):
        return df.withColumn("word_count", count_words(df.content)).orderBy("word_count",ascending=False)
    
    @staticmethod
    @udf(returnType=FloatType())
    def average_counted_word_in_questions(df):
        return Query.get_counted_word_in_questions(df).agg(avg("word_count")).collect()[0][0]


