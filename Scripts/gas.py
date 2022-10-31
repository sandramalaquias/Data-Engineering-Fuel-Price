import pandas as pd
import seaborn as sns
from pandas_profiling import ProfileReport
import matplotlib.pyplot as plt
import configparser, argparse
import sys
import matplotlib.dates as mdates

""" get path to data """
# Initiate the parser
parser = argparse.ArgumentParser(description='Path for input/output/config files')

# Add long and short argument
parser.add_argument("--inputpath", "-inputpath", help="The URI for input data like </home/sandra/code/fuel_project/Data/fuel_result.csv>")
parser.add_argument("--outputpath", "-outputpath", help="The URI for output data like </home/sandra/code/fuel_project/Data/>")
    
#Get arguments from the command line
args = parser.parse_args()

# Check for --inputpath
if args.inputpath:
    inputpath=args.inputpath
else:
    inputpath = 'error'

if args.outputpath:
    outputpath=args.outputpath
else:
    outputpath = ''

## read file
try:
    df = pd.read_csv(inputpath)
except FileNotFoundError:
    print(f'Your path - {path} - is no valid path.')
    sys.exit()
    

## new columns to get plot
df['purchase_power'] = df.wage_value / df.fuel_sales_price
df['date'] = pd.to_datetime(df['fuel_year_month'], format='%Y%m')
df['tax_acum'] = df['ipca_tax'].cumsum()

## write stats htlm 
profile = df.profile_report(title='teste')
path = outputpath + 'fuel_gas.html'
profile.to_file(output_file=path)

# Set the locator
locator = mdates.MonthLocator()  # every month
# Specify the format - %b gives us Jan, Feb...
fmt = mdates.DateFormatter('%b %Y')

##plot 1 - fuel and indicators
plt.figure(figsize=(25, 10))
plt.plot(df.date, df.fuel_sales_price, color='red', label='fuel')
plt.plot(df.date, df.brent_avg_value/158.98, color='black', label='brent-litro')
plt.plot(df.date, df.usd_avg_sale, color='blue', label='usd')
plt.plot(df.date, df.purchase_power/100, color='magenta', label='purchase power p/ 100')

plt.title("Fuel Price Timeline")
plt.legend(loc="upper left")
X = plt.gca().xaxis
X.set_major_locator(locator)
# Specify formatter
X.set_major_formatter(fmt)
ticks = list(df['date'])
plt.xticks([ticks[i] for i in range(len(ticks)) if i % 3 == 0], rotation=45)
path = outputpath + 'fuel_price_timeline.jpg'
plt.savefig(path)
plt.show()

##plot 2 - fuel and purchase power

plt.figure(figsize=(25, 6))
plt.plot(df.date, df.tax_acum, color='black', label='IPCA acumulado')
plt.plot(df.date, df.wage_value, color='blue', label='wage')
plt.plot(df.date, df.purchase_power, color='magenta', label='purchase power')

plt.title("Purchase Power Timeline")
plt.legend(loc="upper left")
X = plt.gca().xaxis
X.set_major_locator(locator)
# Specify formatter
X.set_major_formatter(fmt)
ticks = list(df['date'])
plt.xticks([ticks[i] for i in range(len(ticks)) if i % 3 == 0], rotation=45)
path = outputpath + "purchase_power_timeline.jpg"
plt.savefig(path)
plt.show()


##plot 3 - fuel and dolar timeline

plt.figure(figsize=(25, 6))
plt.plot(df.date, df.fuel_sales_price, color='red', label='fuel')
plt.plot(df.date, df.usd_avg_sale, color='blue', label='usd')

plt.title("Fuel Price Timeline")
plt.legend(loc="upper left")
X = plt.gca().xaxis
X.set_major_locator(locator)
# Specify formatter
X.set_major_formatter(fmt)
ticks = list(df['date'])
plt.xticks([ticks[i] for i in range(len(ticks)) if i % 3 == 0], rotation=45)
path = outputpath + "graph_fuel_usd_timeline.jpg"
plt.savefig(path)
plt.show()
