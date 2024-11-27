import json


def filter_records_by_stock_name(file_name, stock_name):
    # 读取JSON文件并指定编码为utf-8
    with open(file_name, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # 筛选出符合股票名字的记录
    filtered_records = [record for record in data if record[0] == stock_name]

    return filtered_records


if __name__ == "__main__":
    file_name = 'static/data/analysis_result.json'
    stock_name = '奥雅股份'  # 替换为你要筛选的股票名称
    filtered_records = filter_records_by_stock_name(file_name, stock_name)

    # 打印筛选结果
    print(filtered_records)
