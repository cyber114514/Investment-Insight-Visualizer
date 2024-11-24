from flask import render_template
from flask import Flask
import plot_data

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/<filename>')
def req_file(filename):
    return render_template(filename)

@app.route('/tradeDaysPrice.html')
def show_trade_days_price():
    plot_data.plot_trade_days_price()
    return render_template('tradeDaysPrice.html', image_file='trade_days_price.png')

@app.route('/weekPriceChange.html')
def show_week_price_change():
    plot_data.plot_week_price_change()
    return render_template('weekPriceChange.html', image_file='week_price_change.png')

if __name__ == '__main__':
    app.DEBUG = True
    app.jinja_env.auto_reload = True
    app.run()
