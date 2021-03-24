# test
# 1. function1 读入数据
def read_data(url):
    print("[*] Read data from {}".format(url))
    df = spark.read.csv(url,header='true')
    return df
# function1 调用方法

# 浏览桂格和竞品后购买桂格 61 columns
url1 = "s3a://01253198f70d4ae531c0e2eeb9e83342/readonly/hive/crowd_feature_9n_8c18fe10886311ebae41fa163e446b35_20210319120117"
# 浏览和购买桂格产品线 307 columns
url2 = "s3a://01253198f70d4ae531c0e2eeb9e83342/readonly/hive/crowd_feature_9n_0797497e886a11eb9705fa163eb774d5_20210319122544"
df1=read_data(url1)
df2=read_data(url2)
