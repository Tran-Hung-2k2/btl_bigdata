import json

def read_dataframe_from_s3(spark, url):
    return spark.read.json(url)

def save_dataframes_to_elasticsearch(dataframes,indices,es_write_config):
    #    dataframes: list of all PySpark DataFrames
    #    indices: list of elasticsearch indices
    #    es_write_config: dict of elasticsearch write config

    for dataframe,index in zip(dataframes,indices):
        print("Processing index:",index)
        es_write_config['es.resource'] = index
        rdd_ = dataframe.rdd
        rdd_.map(lambda row: (None, \
                              json.dumps(row.asDict()))) \
                              .saveAsNewAPIHadoopFile(path='-', \
                              outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat", \
                              keyClass="org.apache.hadoop.io.NullWritable", \
                              valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", \
                              conf=es_write_config)

