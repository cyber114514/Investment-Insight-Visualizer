from pyspark import SparkContext
from pyspark.sql import SparkSession
import json


# 统计交易次数大于500的基金/股票名称，并按交易次数从多到少排序前20名
def fundStockHighTrade(sc, spark, df):
    j = df.groupBy('基金/股票名称').sum('七天内交易次数').withColumnRenamed('sum(七天内交易次数)', 'total_trades') \
        .filter('total_trades > 500').orderBy('total_trades', ascending=False).take(20)
    with open('static/data/fundStockHighTrade.json', 'w') as f:
        f.write(json.dumps(j))


# 统计各基金/股票的涨跌总金额
def fundStockPriceChange(sc, spark, df):
    j = df.select('基金/股票名称', '涨跌金额 (元)').rdd \
        .map(lambda v: (v['基金/股票名称'], float(v['涨跌金额 (元)']))) \
        .reduceByKey(lambda x, y: x + y).collect()
    with open('static/data/fundStockPriceChange.json', 'w') as f:
        f.write(json.dumps(j))


# 统计每只基金/股票的平均盘价，并返回键值对格式
def averagePrice(sc, spark, df):
    result = df.groupBy('基金/股票名称').avg('盘价 (元)').withColumnRenamed('avg(盘价 (元))', 'avg_price').orderBy(
        'avg_price', ascending=False).collect()
    ans = {v['基金/股票名称']: v['avg_price'] for v in result}
    with open('static/data/averagePrice.json', 'w') as f:
        f.write(json.dumps(ans, indent=2, ensure_ascii=False))


# 取出总交易次数排名前20的基金/股票名称
def topTradeList(sc, spark, df):
    top_trade_list = df.groupBy('基金/股票名称').sum('七天内交易次数').withColumnRenamed('sum(七天内交易次数)',
                                                                                         'total_trades') \
        .orderBy('total_trades', ascending=False).rdd.map(lambda v: v[0]).take(20)
    return top_trade_list


# 分析总交易次数前20的基金/股票的近一周涨跌情况
def weekPriceChange(sc, spark, df, top_trade_list):
    result = df.select('基金/股票名称', '涨跌百分比 (%)', '涨跌金额 (元)').rdd \
        .filter(lambda v: v['基金/股票名称'] in top_trade_list) \
        .map(lambda v: (v['基金/股票名称'], (float(v['涨跌百分比 (%)']), float(v['涨跌金额 (元)'])))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).collect()

    result = list(map(lambda v: [v[0], v[1][0], v[1][1]], result))
    ans = {}
    for name in top_trade_list:
        ans[name] = list(filter(lambda v: v[0] == name, result))
    with open('static/data/weekPriceChange.json', 'w') as f:
        f.write(json.dumps(ans))


# 高交易量基金/股票建议
def highVolumeRecommendation(sc, spark, df):
    result = df.groupBy('基金/股票名称').sum('七天内交易次数').withColumnRenamed('sum(七天内交易次数)', 'total_trades') \
        .orderBy('total_trades', ascending=False).take(20)
    with open('static/data/highVolumeRecommendation.json', 'w') as f:
        f.write(json.dumps(result))


# 平均盘价稳定且上升趋势明显的基金/股票建议
def stablePriceRecommendation(sc, spark, df):
    result = df.groupBy('基金/股票名称').avg('盘价 (元)').withColumnRenamed('avg(盘价 (元))', 'avg_price') \
        .filter('avg_price > 100').orderBy('avg_price', ascending=False).take(20)
    with open('static/data/stablePriceRecommendation.json', 'w') as f:
        f.write(json.dumps(result))


# 近一周内上涨趋势明显的基金/股票建议（基于整个数据集）
def weeklyUpwardTrendRecommendation(sc, spark, df):
    rdd = df.select('基金/股票名称', '涨跌百分比 (%)').rdd \
        .filter(lambda v: float(v['涨跌百分比 (%)']) > 0) \
        .map(lambda v: (v['基金/股票名称'], float(v['涨跌百分比 (%)']))) \
        .reduceByKey(lambda x, y: x + y)

    # 转换为DataFrame以进行排序
    result = spark.createDataFrame(rdd, schema=['基金/股票名称', '总涨跌百分比'])
    result = result.orderBy(result['总涨跌百分比'], ascending=False).take(20)

    with open('static/data/weeklyUpwardTrendRecommendation.json', 'w') as f:
        f.write(json.dumps([row.asDict() for row in result]))


# 总交易次数前五的基金/股票在不同天数交易次数中的平均盘价
def tradeDaysPrice(sc, spark, df, top_trade_list):
    result = df.select('基金/股票名称', '七天内交易次数', '盘价 (元)').rdd \
        .filter(lambda v: v['基金/股票名称'] in top_trade_list) \
        .map(lambda v: (v['基金/股票名称'], (float(v['盘价 (元)']), 1))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda v: (v[0], v[1][0] / v[1][1])).collect()

    with open('static/data/tradeDaysPrice.json', 'w') as f:
        f.write(json.dumps(result))


# 代码入口
if __name__ == "__main__":
    sc = SparkContext('local', 'InvestmentAnalysis')
    sc.setLogLevel("WARN")
    spark = SparkSession.builder.appName("InvestmentAnalysis").getOrCreate()
    file = "investment_data.csv"
    df = spark.read.csv(file, header=True)  # dataframe
    # 将列转换为数值类型
    df = df.withColumn("七天内交易次数", df["七天内交易次数"].cast("int"))
    df = df.withColumn("盘价 (元)", df["盘价 (元)"].cast("float"))
    df = df.withColumn("涨跌百分比 (%)", df["涨跌百分比 (%)"].cast("float"))
    df = df.withColumn("涨跌金额 (元)", df["涨跌金额 (元)"].cast("float"))

    top_trade_list = topTradeList(sc, spark, df)

    fundStockHighTrade(sc, spark, df)
    fundStockPriceChange(sc, spark, df)
    averagePrice(sc, spark, df)
    weekPriceChange(sc, spark, df, top_trade_list)
    tradeDaysPrice(sc, spark, df, top_trade_list)

    highVolumeRecommendation(sc, spark, df)
    stablePriceRecommendation(sc, spark, df)
    weeklyUpwardTrendRecommendation(sc, spark, df)
