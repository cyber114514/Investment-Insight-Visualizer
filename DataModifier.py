import pandas as pd


def clean_stock_data(file_name):
    # 读取数据文件
    df = pd.read_csv(file_name)

    # 筛选并删除换手率为0.0的记录
    df_cleaned = df[df['换手率'] != 0.0]

    # 保存清理后的数据到新文件
    cleaned_file_name = 'cleaned_stock_data.csv'
    df_cleaned.to_csv(cleaned_file_name, index=False)

    return cleaned_file_name


if __name__ == "__main__":
    file_name = 'stock_data.csv'
    cleaned_file_name = clean_stock_data(file_name)
    print(f"Cleaned data saved to {cleaned_file_name}")
