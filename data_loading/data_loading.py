import pandas as pd
    
raw_interprov_expenditure = pd.read_csv("data/interprovincial_tourism_expenditures/24100044.csv")
raw_foreign_spending = pd.read_csv("data/spending_by_foreign_tourists/24100047.csv")
raw_tourism_expenditure = pd.read_csv("data/tourism_supply_expenditure/24100004.csv")
raw_tourists_in_canada = pd.read_csv("data/tourists_entering_canada/24100050.csv", dtype={'STATUS': str, 'TERMINATED': str})

print(raw_interprov_expenditure.isna().sum())

"""
- All STATUS and SYMBOL column values are NA, meaning there are no flags in this data
- TERMINATED column values are NA - this is irrelevant to us anyway since we do not need annual updates
- DECIMALS is always 1 since all values are rounded to 1 decimal place
- UOM_ID is StatCan's code for "Dollars", and UOM is always Dollars
- SCALAR_ID is the code for "Millions", and SCALAR_FACTOR is always millions
- COORDINATE is not helpful for our purposes
"""

def splitValues(x):
    split = x.split(',')
    return split[0]

def spendingSplitValues(x):
    split = x.split(' in ')
    return split[1]

def mapToMillions(x):
    return x * 1000000

raw_interprov_expenditure['GEO'] = raw_interprov_expenditure['GEO'].map(splitValues)
raw_interprov_expenditure['Geography, location of the tourism spending'] = raw_interprov_expenditure['Geography, location of the tourism spending'].map(spendingSplitValues)
raw_interprov_expenditure['VALUE'] = raw_interprov_expenditure['VALUE'].map(mapToMillions)

interprov_expenditure = raw_interprov_expenditure[['REF_DATE', 'GEO', 'Geography, location of the tourism spending', 'VALUE']].copy()

interprov_expenditure.columns = ['Year', 'Province of Origin', 'Destination Province', 'Amount Spent']



