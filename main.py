from pyspark import SparkContext
from pyspark.sql import SparkSession
import json


def filter_high_trade_stocks(df):
    return df.filter(df['成交量'] > 500).orderBy('成交量', ascending=False)


def determine_risk_type(user_capital):
    if user_capital < 10000:
        return '极端保守型'
    elif 10000 <= user_capital < 50000:
        return '保守型'
    elif 50000 <= user_capital < 200000:
        return '稳健型'
    elif 200000 <= user_capital < 500000:
        return '平衡型'
    elif 500000 <= user_capital < 1000000:
        return '激进型'
    else:
        return '极端激进型'


def filter_by_risk(df, risk_type):
    if risk_type == '极端保守型':
        return df.filter((df['换手率'] < 5) & (df['市盈率'] < 15) & (df['成交量'] > 10000))
    elif risk_type == '保守型':
        return df.filter((df['振幅'] < 2) & (df['市盈率'] < 20) & (df['换手率'] > 3))
    elif risk_type == '稳健型':
        return df.filter((df['市盈率'] < 25) & (df['涨跌幅'] < 5) & (df['成交额'] > 500000))
    elif risk_type == '平衡型':
        return df.filter((df['市盈率'] < 30) & (df['振幅'] < 5) & (df['成交量'] > 50000))
    elif risk_type == '激进型':
        return df.filter((df['涨跌幅'] > 5) & (df['换手率'] > 10) & (df['市盈率'] < 40))
    elif risk_type == '极端激进型':
        return df.filter((df['涨跌幅'] > 10) & (df['振幅'] > 5) & (df['成交额'] > 1000000))
    else:
        return df


def match_stocks_for_beginners(df, user_capital):
    # 根据用户本金判断风险承受类型
    risk_type = determine_risk_type(user_capital)

    # 筛选符合基本条件的股票
    df_filtered = df.filter(
        (df['最低价'] < df['今开']) &
        (df['最新价'] > df['今开']) &
        (df['今开'] > df['昨开'])
    )

    # 根据风险承受类型进一步筛选股票
    df_filtered = filter_by_risk(df_filtered, risk_type)

    return df_filtered


def analyze_stocks(df):
    # 分析：计算总交易次数、价格变动等
    result = df.groupBy('股票名称').agg(
        {'成交量': 'sum', '涨跌额': 'sum', '最新价': 'avg'}
    ).withColumnRenamed('sum(成交量)', 'total_trades') \
        .withColumnRenamed('sum(涨跌额)', 'total_price_change') \
        .withColumnRenamed('avg(最新价)', 'avg_price').collect()

    # 将分析结果保存到JSON文件
    with open('static/data/analysis_result.json', 'w') as f:
        f.write(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    sc = SparkContext('local', 'InvestmentAnalysis')
    sc.setLogLevel("WARN")
    spark = SparkSession.builder.appName("InvestmentAnalysis").getOrCreate()
    file = "cleaned_stock_data.csv"
    df = spark.read.csv(file, header=True)

    # 将列转换为数值类型
    df = df.withColumn("成交量", df["成交量"].cast("int"))
    df = df.withColumn("换手率", df["换手率"].cast("float"))
    df = df.withColumn("市盈率", df["市盈率"].cast("float"))
    df = df.withColumn("振幅", df["振幅"].cast("float"))
    df = df.withColumn("涨跌幅", df["涨跌幅"].cast("float"))
    df = df.withColumn("成交额", df["成交额"].cast("float"))
    df = df.withColumn("最新价", df["最新价"].cast("float"))
    df = df.withColumn("涨跌额", df["涨跌额"].cast("float"))
    df = df.withColumn("最低价", df["最低价"].cast("float"))
    df = df.withColumn("今开", df["今开"].cast("float"))
    df = df.withColumn("昨开", df["昨开"].cast("float"))

    user_capital = 100000  # 示例用户本金

    high_trade_stocks = filter_high_trade_stocks(df)
    matching_stocks = match_stocks_for_beginners(high_trade_stocks, user_capital)

    # 对匹配的股票进行进一步分析
    analyze_stocks(matching_stocks)
