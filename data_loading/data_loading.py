import pandas as pd
    
raw_interprov_expenditure = pd.read_csv("data/interprovincial_tourism_expenditures/24100044.csv")
raw_foreign_spending = pd.read_csv("data/spending_by_foreign_tourists/24100047.csv")
raw_tourism_expenditure = pd.read_csv("data/tourism_supply_expenditure/24100004.csv")
raw_tourists_in_canada = pd.read_csv("data/tourists_entering_canada/24100050.csv", dtype={'STATUS': str, 'TERMINATED': str})

# # --------- Cleaning of raw_interprov_expenditure -------
# print(raw_interprov_expenditure.isna().sum())
# print((raw_interprov_expenditure['SCALAR_FACTOR'] == 'millions').sum())
# print((raw_interprov_expenditure['DECIMALS'] == 0).sum())

# """
# - All STATUS and SYMBOL column values are NA, meaning there are no flags in this data
# - TERMINATED column values are NA - this is irrelevant to us anyway since we do not need annual updates
# - DECIMALS is always 1 since all values are rounded to 1 decimal place
# - UOM_ID is StatCan's code for "Dollars", and UOM is always Dollars
# - SCALAR_ID is the code for "Millions", and SCALAR_FACTOR is always millions
# - COORDINATE is not helpful for our purposes
# """

# def splitValues(x):
#     split = x.split(',')
#     return split[0]

# def spendingSplitValues(x):
#     split = x.split(' in ')
#     return split[1]

# def mapToMillions(x):
#     return x * 1000000

# raw_interprov_expenditure['GEO'] = raw_interprov_expenditure['GEO'].map(splitValues)
# raw_interprov_expenditure['Geography, location of the tourism spending'] = raw_interprov_expenditure['Geography, location of the tourism spending'].map(spendingSplitValues)
# raw_interprov_expenditure['VALUE'] = raw_interprov_expenditure['VALUE'].map(mapToMillions)

# interprov_expenditure = raw_interprov_expenditure[['REF_DATE', 'GEO', 'Geography, location of the tourism spending', 'VALUE']].copy()

# interprov_expenditure.columns = ['Year', 'Province of Origin', 'Destination Province', 'Amount Spent']

# print(raw_foreign_spending.isna().sum())
# print(raw_foreign_spending.shape)

# print((raw_foreign_spending['SCALAR_FACTOR'] == 'thousands').sum())
# print((raw_foreign_spending['DECIMALS'] == 0).sum())
# print((raw_foreign_spending['UOM'] == 'Dollars').sum())
# print(raw_foreign_spending['STATUS'].isin(['F', 'x', '...', '..']).sum())

# """
# - None of the STATUS colums values are unusuable, therefore we don't need to filter out any rows
# - TERMINATED column values are NA - this is irrelevant to us anyway since we do not need annual updates
# - DECIMALS is always 0 since all values are rounded to whole numbers
# - UOM_ID is StatCan's code for "Dollars", and UOM is always Dollars
# - SCALAR_ID is the code for "Thousands", and SCALAR_FACTOR is always "thousands"
# - COORDINATE is not helpful for our purposes
# - DGUID has NA values, however we don't need this
# - Drop totals and make a new DF for this

# We end up with two dataframes:
# - totals_foreign_spending (aggregates amount spend by region visited)
# - foreign_spending (this is our main dataframe)
# """

# def removeResidents(x):
#     if 'residents' in x:
#         return x.replace('residents', '')
#     return x
    
# def mapToThousands(x):
#     return x * 1000

# # keep aggregations of tourist origins as a separate dataframe
# query = "`Area of residence` == 'Total, area of residence'"
# raw_totals_foreign_spending = pd.DataFrame(raw_foreign_spending.query(query)).reset_index(drop=True)
# raw_totals_foreign_spending['VALUE'] = raw_totals_foreign_spending['VALUE'].map(mapToThousands)
# totals_foreign_spending = raw_totals_foreign_spending[['REF_DATE', 'GEO', 'Type of expenditures', 'VALUE']].copy()
# totals_foreign_spending.columns = ['Date', 'Region Visited', 'Expenditure Type', 'Amount Spent']

# # remove aggregations from main dataframe
# raw_foreign_spending = raw_foreign_spending.query("`Area of residence` != 'Total, area of residence'").reset_index(drop=True)

# # remove extra noise from 'Place of Residence' column for improved legibility
# raw_foreign_spending['Area of residence'] = raw_foreign_spending['Area of residence'].map(removeResidents)

# # map amount spent to thousands
# raw_foreign_spending['VALUE'] = raw_foreign_spending['VALUE'].map(mapToThousands)

# # new dataframe keeping only columns we want
# foreign_spending = raw_foreign_spending[['REF_DATE', 'GEO', 'Area of residence', 'Type of expenditures', 'VALUE']].copy()

# # rename columns for legibility
# foreign_spending.columns = ['Date', 'Region Visited', 'Place of Residence', 'Expenditure Type', 'Amount Spent']

# print(foreign_spending.head())


# ----- Tourism Expenditure Cleaning ------
"""
- We have 2163 rows that have STATUS == '..', which means 'not available'. These rows must be filtered out
- After filtering out the rows, there are no more NA values for the VALUE column
- All remaining STATUS and SYMBOL column values are NA, meaning there are no more flags in this data
- TERMINATED column values are NA - this is irrelevant to us anyway since we do not need annual updates
- DECIMALS is always 1 since all values are rounded to 1 decimal place
- Column names are renamed for clarity
- Values are not mapped to millions due to the numbers being too large to quickly read and understand
- Products with aggregations of subproducts were filtered out

We end up with the dataframe:
- tourism_expenditures 
"""
# count NA values across columns and compare against data shape
print(raw_tourism_expenditure.isna().sum())
print(raw_tourism_expenditure.shape)

# ensure that all units are the same across rows
print((raw_tourism_expenditure['SCALAR_FACTOR'] == 'millions').sum())
print((raw_tourism_expenditure['DECIMALS'] == 1).sum())
print((raw_tourism_expenditure['UOM'] == 'Dollars').sum())

# check if there is any unusable data
print(raw_tourism_expenditure['STATUS'].isin(['F', 'x', '...', '..']).sum())

# see which flag values are in 'STATUS' so we can filter them out
print(raw_tourism_expenditure['STATUS'].unique())

# see what other values of 'UOM' there are besides 'Dollars' so we can filter them out
print(raw_tourism_expenditure['UOM'].unique())

# filter out rows with flags
raw_tourism_expenditure = raw_tourism_expenditure.query("STATUS != '..'").reset_index(drop=True)
print(raw_tourism_expenditure.isna().sum())

# filter our rows in 'UOM' that aren't 'Dollars' ('Percentage' was a value in for 'Indicators' == 'Tourism product ration')
raw_tourism_expenditure = raw_tourism_expenditure.query("UOM != 'Percentage'").reset_index(drop=True)
print(raw_tourism_expenditure.query("UOM == 'Percentage'"))

# filter out rows that are aggregations of other rows
raw_tourism_expenditure = raw_tourism_expenditure[~raw_tourism_expenditure['Products'].str.startswith("Total")]

# see all possible values for 'Indicators' column to see how to best name groups for clarity
print(raw_tourism_expenditure['Indicators'].unique())

# filter out rows where 'Indicators' == 'total demand', 'exports', or 'imports' since these are aggregation of existing values in the dataset
raw_tourism_expenditure = raw_tourism_expenditure.query("(Indicators != 'Total demand') & (Indicators != 'Exports') & (Indicators != 'Imports')")

# rename remaining columns
def renameIndicator(x):
    if x == 'Total domestic supply':
        return 'Total local supply'
    elif x == 'Domestic demand':
        return 'Local spending'
    elif x == 'Interprovincial demand (exports)':
        return 'Domestic visitor spending'
    elif x == 'International imports':
        return 'Sourced from abroad'
    elif x == 'International demand (exports)':
        return 'Foreign visitor spending'
    elif x == 'Interprovincial imports':
        return 'Sourced from other provinces'
    return x

raw_tourism_expenditure['Indicators'] = raw_tourism_expenditure['Indicators'].map(renameIndicator);

# new dataframe with clean data
tourism_expenditure = raw_tourism_expenditure[['REF_DATE', 'GEO', 'Indicators', 'Products', 'VALUE']]

# rename columns for clarity
tourism_expenditure.columns = ['Year', 'Province', 'Economic Measure', 'Product', 'Value (Millions)']

print(tourism_expenditure.head())
print(tourism_expenditure.shape)
    