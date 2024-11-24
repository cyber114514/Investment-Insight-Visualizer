import matplotlib.pyplot as plt
import json

def plot_trade_days_price():
    with open('static/data/tradeDaysPrice.json', 'r') as f:
        data = json.load(f)

    names = [item[0] for item in data]
    prices = [item[1] for item in data]

    plt.figure(figsize=(10, 5))
    plt.bar(names, prices, color='blue')
    plt.xlabel('Fund/Stock Name')
    plt.ylabel('Average Price')
    plt.title('Average Price per Trading Days for Top 20 Funds/Stocks by Trading Volume')
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.savefig('static/images/trade_days_price.png')
    plt.close()

def plot_week_price_change():
    with open('static/data/weekPriceChange.json', 'r') as f:
        data = json.load(f)

    names = list(data.keys())
    percentages = [data[name][0][1] for name in names]
    amounts = [data[name][0][2] for name in names]

    fig, ax1 = plt.subplots(figsize=(12, 6))

    color = 'tab:blue'
    ax1.set_xlabel('Fund/Stock Name')
    ax1.set_ylabel('Percentage Change (%)', color=color)
    ax1.plot(names, percentages, color=color, marker='o', label='Percentage Change (%)')
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.legend(loc='upper left')

    ax2 = ax1.twinx()
    color = 'tab:red'
    ax2.set_ylabel('Change Amount (¥)', color=color)
    ax2.plot(names, amounts, color=color, marker='x', label='Change Amount (¥)')
    ax2.tick_params(axis='y', labelcolor=color)
    ax2.legend(loc='upper right')

    plt.title('Weekly Percentage and Amount Change for Top 20 Funds/Stocks by Trading Volume')
    plt.xticks(rotation=90)
    fig.tight_layout()
    plt.savefig('static/images/week_price_change.png')
    plt.close()

if __name__ == "__main__":
    plot_trade_days_price()
    plot_week_price_change()
