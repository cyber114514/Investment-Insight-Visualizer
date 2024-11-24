import csv
import random
import string


def generate_unique_name(existing_names, length=8):
    while True:
        name = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
        if name not in existing_names:
            return name


headers = ["基金/股票名称", "盘价 (元)", "七天内交易次数", "涨跌百分比 (%)", "涨跌金额 (元)"]
existing_names = set()

with open('investment_data.csv', 'w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(headers)

    for _ in range(5000):
        name = generate_unique_name(existing_names)
        existing_names.add(name)
        price = round(random.uniform(5, 2000), 2)
        trades = random.randint(0, 1000)
        change_percent = round(random.uniform(-100, 100), 2)
        change_amount = round(price * (change_percent / 100), 2)
        writer.writerow([name, price, trades, change_percent, change_amount])

print("数据集已生成并保存为 investment_data.csv")
