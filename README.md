# Investment-Insight-Visualizer

## 项目概述
“Investment-Insight-Visualizer”旨在通过可视化投资数据，为交易模式、价格变动和基金/股票投资推荐提供洞察。该项目主要用于展示利用分布式计算框架Hadoop分离聚合后的数据，利用Matplotlib进行数据可视化，并通过Flask框架来展示网页。

## 项目结构
- `SparkFlask.py`：主Flask应用程序。
- `plot_data.py`：负责使用Matplotlib生成图表的脚本。
- `templates/`：存放HTML模板的目录。
- `static/data/`：存放JSON数据文件的目录。
- `static/images/`：存放生成的图像的目录。

## 可视化
项目利用matplot绘图提供以下可视化功能：<br> 
`（开发时注意展示图片,启动项目时会自动生成图片，资源在static/images/中，文件名为trade_days_price.png、week_price_change.png）`
1. 交易天数与盘价： 显示前20名基金/股票在不同交易天数中的平均盘价。
2. 近一周涨跌情况： 显示过去一周内前20名基金/股票的涨跌百分比和涨跌金额。

## 运行项目
1. 确保已安装Python和所需的库：
   `pip install flask matplotlib`
2. 运行Flask应用程序：
   `python SparkFlask.py`
4. 在浏览器中访问：
   `http://127.0.0.1:5000`
## 数据文件
该项目使用多个JSON数据文件来存储和可视化投资数据。每个文件具有特定的格式和用途。

### 1. `fundStockHighTrade.json`
**描述：** 列出了交易次数超过500次的基金/股票，按交易次数降序排列。
**格式：**
[
  {
    "基金/股票名称": "ExampleFund",
    "total_trades": 750
  },
  ...
]

### 2. `fundStockPriceChange.json（注意！这个有2000条数据，不用一次性全部展示）`
**描述：** 包含每个基金/股票的涨跌总金额。
**格式：**[
  {
    "基金/股票名称": "ExampleFund",
    "涨跌金额 (元)": 500.75
  },
  ...
]

### 3. `averagePrice.json（注意！这个有2000条数据，不用一次性全部展示）`
**描述：** 包含每个基金/股票的平均盘价。 
**格式：**{
  "ExampleFund": 300.75,
  ...
}

### 4. `tradeDaysPrice.json`
**描述：** 前20名基金/股票在不同交易天数中的平均盘价。 
**格式：**[
  [
    "ExampleFund",
    150.75
  ],
  ...
]

### 5. `weekPriceChange.json`
**描述：** 前20名基金/股票的近一周涨跌百分比和涨跌金额。 
**格式：**{
  "ExampleFund": [
    [
      "ExampleFund",
      2.75,
      15.50
    ]
  ],
  ...
}

### 6. `highVolumeRecommendation.json`
**描述：** 高交易量基金/股票的推荐。 
**格式：**[
  {
    "基金/股票名称": "ExampleFund",
    "total_trades": 800
  },
  ...
]

### 7. `stablePriceRecommendation.json`
**描述：** 价格稳定且上升趋势明显的基金/股票的推荐。 
**格式：**[
  {
    "基金/股票名称": "ExampleFund",
    "avg_price": 350.75
  },
  ...
]

### 8. `weeklyUpwardTrendRecommendation.json`
**描述：** 过去一周内上升趋势明显的基金/股票的推荐。 
**格式：**[
  {
    "基金/股票名称": "ExampleFund",
    "总涨跌百分比": 4.75
  },
  ...
]


