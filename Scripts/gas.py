import pandas as pd
import seaborn as sns
from pandas_profiling import ProfileReport
import matplotlib.pyplot as plt
import configparser, argparse
import sys

""" get path to data """
# Initiate the parser
parser = argparse.ArgumentParser(description='Path for input/output/config files')

# Add long and short argument
parser.add_argument("--inputpath", "-inputpath", help="The URI for data like '/home/sandra/Downloads/fuel_result.csv'")
    
# Read arguments from the command line
args = parser.parse_args()

# Check for --inputpath
if args.inputpath:
    path=args.inputpath
else:
    path = 'error'

try:
    df = pd.read_csv(path)
except FileNotFoundError:
    print(f'Your path - {path} - is no valid path.')
    sys.exit()

df['purchase_power'] = df.wage_value / df.fuel_sales_price
profile = df.profile_report(title='teste')
profile.to_file(output_file="fuel_gas.html")
plt.figure(figsize=(20, 10))
plt.xlabel('period', fontsize=10)
plt.plot(df.fuel_year_month, df.fuel_sales_price, color='red', label='fuel')
plt.plot(df.fuel_year_month, df.brent_avg_value/100, color='black', label='brent / 100')
plt.plot(df.fuel_year_month, df.usd_avg_sale, color='blue', label='usd')
plt.plot(df.fuel_year_month, df.purchase_power/100, color='magenta', label='purchase power / 100')
plt.plot(df.fuel_year_month, df.wage_value/100, color='cyan', label='minimum wage / 100')
plt.plot(df.fuel_year_month, df.ipca_tax, color='orange', label='ipca tax')
plt.title("Fuel Price Timeline")
plt.legend(loc="upper left")
plt.savefig("graph_fuel_timeline.jpg")