import pandas as pd


# --------- raw_interprov_expenditure cleaning (dataset 1) -------
def load_and_clean_dataset1():
    """
    - All STATUS and SYMBOL column values are NA, meaning there are no flags in this data
    - TERMINATED column values are NA - this is irrelevant to us anyway since we do not need annual updates
    - DECIMALS is always 1 since all values are rounded to 1 decimal place
    - UOM_ID is StatCan's code for "Dollars", and UOM is always Dollars
    - SCALAR_ID is the code for "Millions", and SCALAR_FACTOR is always millions
    - COORDINATE is not helpful for our purposes
    """
    
    raw_interprov_expenditure = pd.read_csv("data/interprovincial_tourism_expenditures/24100044.csv")
    
    print(raw_interprov_expenditure.isna().sum())
    print((raw_interprov_expenditure['SCALAR_FACTOR'] == 'millions').sum())
    print((raw_interprov_expenditure['DECIMALS'] == 0).sum())

    def splitValues(x):
        split = x.split(',')
        return split[0]

    def spendingSplitValues(x):
        split = x.split(' in ')
        return split[1]

    def mapToMillions(x):
        return x * 1000000

    # removing 'residence; from GEO (province of origin) for clarity
    raw_interprov_expenditure['GEO'] = raw_interprov_expenditure['GEO'].map(splitValues)

    # removing 'Spending in' from 'Geography, location of the tourism spending' column - so we are just left with province names
    raw_interprov_expenditure['Geography, location of the tourism spending'] = raw_interprov_expenditure['Geography, location of the tourism spending'].map(spendingSplitValues)

    # mapping values to their proper units (millions)
    raw_interprov_expenditure['VALUE'] = raw_interprov_expenditure['VALUE'].map(mapToMillions)

    # only keep these columns for our cleaned dataframe
    interprovincial_tourist_spending = raw_interprov_expenditure[['REF_DATE', 'GEO', 'Geography, location of the tourism spending', 'VALUE']].copy()

    # rename columns
    interprovincial_tourist_spending.columns = ['year', 'province_of_residence', 'destination_province', 'amount_spent']

    print("\n------- Cleaned Dataset 1 Preview -------- ")
    print(interprovincial_tourist_spending.head())

    return interprovincial_tourist_spending





# ----------- raw_foreign_spending cleaning (dataset 2) ------------
def load_and_clean_dataset2():
    """
    - None of the STATUS colums values are unusuable, therefore we don't need to filter out any rows
    - TERMINATED column values are NA - this is irrelevant to us anyway since we do not need annual updates
    - DECIMALS is always 0 since all values are rounded to whole numbers
    - UOM_ID is StatCan's code for "Dollars", and UOM is always Dollars
    - SCALAR_ID is the code for "Thousands", and SCALAR_FACTOR is always "thousands"
    - COORDINATE is not helpful for our purposes
    - DGUID has NA values, however we don't need this
    - Drop totals and make a new DF for this
    - Remove Canada-wide data since it isn't helpful and it likely aggregations of the provincial data
    - Remove territories-wide aggregations

    *** NOTE *** 
    This dataset also contains some data for internation spending by region in BC and Ontario. Leaving this in in case we do want to try to look into the BC angle

    We end up with two dataframes:
    - totals_foreign_spending (aggregates amount spend by region visited)
    - foreign_spending (this is our main dataframe)
    """
    
    raw_foreign_spending = pd.read_csv("data/spending_by_foreign_tourists/24100047.csv")

    # check NA values against dataframe shape
    print("\n", raw_foreign_spending.isna().sum())
    print(raw_foreign_spending.shape)

    # ensure that units are what we expect them to be across the whole dataset
    print((raw_foreign_spending['SCALAR_FACTOR'] == 'thousands').sum())
    print((raw_foreign_spending['DECIMALS'] == 0).sum())
    print((raw_foreign_spending['UOM'] == 'Dollars').sum())
    print(raw_foreign_spending['STATUS'].isin(['F', 'x', '...', '..']).sum()) # no flagged values

    def removeResidents(x):
        if 'residents' in x:
            return x.replace('residents', '')
        return x
        
    def mapToThousands(x):
        return x * 1000

    # keep aggregations of tourist origins as a separate dataframe
    query = "`Area of residence` == 'Total, area of residence'"
    raw_totals_foreign_spending = pd.DataFrame(raw_foreign_spending.query(query)).reset_index(drop=True)
    raw_totals_foreign_spending['VALUE'] = raw_totals_foreign_spending['VALUE'].map(mapToThousands)
    totals_foreign_spending = raw_totals_foreign_spending[['REF_DATE', 'GEO', 'Type of expenditures', 'VALUE']].copy()
    totals_foreign_spending.columns = ['Date', 'Region Visited', 'Expenditure Type', 'Amount Spent']

    # remove aggregations from main dataframe
    raw_foreign_spending = raw_foreign_spending.query("`Area of residence` != 'Total, area of residence'").reset_index(drop=True)
    raw_foreign_spending = raw_foreign_spending[~raw_foreign_spending['Type of expenditures'].str.startswith("Total")].reset_index(drop=True)

    # remove canada-wide aggregations
    raw_foreign_spending = raw_foreign_spending.query("GEO != 'Canada'").reset_index(drop=True)

    # remove territory aggregations
    raw_foreign_spending = raw_foreign_spending.query('GEO != "Yukon, Northwest Territories and Nunavut"')

    # remove extra noise from 'Place of Residence' column for improved legibility
    raw_foreign_spending['Area of residence'] = raw_foreign_spending['Area of residence'].map(removeResidents).str.strip()

    # map amount spent to thousands
    raw_foreign_spending['VALUE'] = raw_foreign_spending['VALUE'].map(mapToThousands)

    # new dataframe keeping only columns we want
    foreign_spending = raw_foreign_spending[['REF_DATE', 'GEO', 'Area of residence', 'Type of expenditures', 'VALUE']].copy()

    # rename columns for legibility
    foreign_spending.columns = ['date', 'region_visited', 'place_of_residence', 'expenditure_type', 'amount_spent']

    print("\n------- Cleaned Dataset 2 Preview -------- ")
    print(foreign_spending.head())

    return foreign_spending




# ----- Tourism Expenditure Cleaning (dataset 3) ------
def load_and_clean_dataset3():
    """
    - We have 2163 rows that have STATUS == '..', which means 'not available'. These rows must be filtered out
    - After filtering out the rows, there are no more NA values for the VALUE column
    - All remaining STATUS and SYMBOL column values are NA, meaning there are no more flags in this data
    - TERMINATED column values are NA - this is irrelevant to us anyway since we do not need annual updates
    - DECIMALS is always 1 since all values are rounded to 1 decimal place
    - Column names are renamed for clarity
    - Values are not mapped to millions due to the numbers being too large to quickly read and understand
    - Products with aggregations of subproducts were filtered out
    - Rows where Expenditure Type contained 'Total', e.g. was an aggregation, were filtered out

    We end up with the dataframe:
    - tourism_expenditures 
    """
   
    raw_tourism_expenditure = pd.read_csv("data/tourism_supply_expenditure/24100004.csv")

    # count NA values across columns and compare against data shape
    print("\n", raw_tourism_expenditure.isna().sum())
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

    # filter our rows in 'UOM' that aren't 'Dollars' ('Percentage' was the other value in 'UOM' - seen where Indicators == 'Tourism product ratio')
    raw_tourism_expenditure = raw_tourism_expenditure.query("UOM != 'Percentage'").reset_index(drop=True)
    print(raw_tourism_expenditure.query("UOM == 'Percentage'"))

    # filter out rows that are aggregations of other rows
    raw_tourism_expenditure = raw_tourism_expenditure[~raw_tourism_expenditure['Products'].str.startswith("Total")].reset_index(drop=True)
    raw_tourism_expenditure = raw_tourism_expenditure[raw_tourism_expenditure['GEO'] != 'Canada'].reset_index(drop=True)

    # see all possible values for 'Indicators' column to see how to best name groups for clarity
    print(raw_tourism_expenditure['Indicators'].unique())

    # filter out rows where 'Indicators' == 'total demand', 'exports', or 'imports' since these are aggregation of existing values in the dataset
    raw_tourism_expenditure = raw_tourism_expenditure.query("(Indicators != 'Total demand') & (Indicators != 'Exports') & (Indicators != 'Imports')")

    # rename remaining Indicators column values
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
    tourism_expenditure = raw_tourism_expenditure[['REF_DATE', 'GEO', 'Indicators', 'Products', 'VALUE']].copy()

    # rename columns for clarity
    tourism_expenditure.columns = ['year', 'province', 'economic_measure', 'product', 'value_in_millions']

    print("\n------- Cleaned Dataset 3 Preview -------- ")
    print(tourism_expenditure.head())

    return tourism_expenditure
    



# -------- raw_tourists_in_canada cleaning (dataset 4) ----------
def load_and_clean_dataset4():
    """
    - Even though not all values of TERMINATED are NA (meaning the dataseries has been discontinued), we can just keep them for our purposes
    - DECIMALS is always 0, makes sense that we are looking at whole numbers since we are counting visitors
    - 541,773 rows with STATUS == '..' were filtered out. These rows were unusable since StatCan wasn't tracking visitors from those countries during that period
    - Rows where GEO == 'Canada' were filtered out, since we are concerned about the province level and don't want any nation-wide aggregation of provincial data in our dataset
    - Rows where 'Country of residence' was an aggregation, or otherwise not helpful, were filtered out
    - Values of 'Country of residence' are combined under matching names
    - Column names are renamed for clarity
    """

    raw_tourists_in_canada = pd.read_csv("data/tourists_entering_canada/24100050.csv", dtype={'STATUS': str, 'TERMINATED': str})

    print("\n", raw_tourists_in_canada.head())

    # count number of values that are NA within each column and compare against data shape
    print(raw_tourists_in_canada.isna().sum())
    print(raw_tourists_in_canada.shape)

    # ensure that all units are the same across rows
    print((raw_tourists_in_canada['SCALAR_FACTOR'] == 'units').sum())
    print((raw_tourists_in_canada['DECIMALS'] == 0).sum())
    print((raw_tourists_in_canada['UOM'] == 'Visitors').sum())

    # check if there is any unusable data
    print(raw_tourists_in_canada['STATUS'].isin(['F', 'x', '...', '..']).sum())

    # see which flag values are in 'STATUS' so we can filter them out
    print(raw_tourists_in_canada['STATUS'].unique())

    # filter out rows with flags (rows with STATUS == '..')
    raw_tourists_in_canada = raw_tourists_in_canada.query("STATUS != '..'").reset_index(drop=True)
    print(raw_tourists_in_canada.isna().sum())

    # filter out when the region is 'Canada' since we are concerned about the province level
    raw_tourists_in_canada = raw_tourists_in_canada.query("GEO != 'Canada'").reset_index(drop=True)

    # see what values of 'Country of residence' there are
    print(raw_tourists_in_canada['Country of residence'].unique())

    # filter out 'Country of residence' column values that are aggregations and not helpful
    exclude = ['Non-resident visitors entering Canada', 
            'Residents of countries other than the United States of America entering Canada',
            'Americas, countries other than the United States of America',
            'North America, countries other than the United States of America',]

    conditions = (raw_tourists_in_canada['Country of residence'].isin(exclude) | raw_tourists_in_canada['Country of residence'].str.contains('n.o.s', na = False))
    raw_tourists_in_canada = raw_tourists_in_canada[~conditions].reset_index(drop=True)

    # rename 'Country of residence' column values, OR filter our aggregations or column values that are not helpful
    def renameCountry(x):
        if x == 'United States of America residents entering Canada':
            return 'United States'
        if x == 'Germany, Federal Republic of':
            return 'Germany'
        if x == 'Saint Martin (French part)' or x == 'Sint Maarten (Dutch part)':
            return 'Saint Martin'
        if 'Antarctica' in x:
            return 'Antarctica'
        if 'Virgin Islands' in x:
            return 'Virgin Islands'
        if 'Congo,' in x:
            return 'Democratic Republic of the Congo'
        return x

    raw_tourists_in_canada['Country of residence'] = raw_tourists_in_canada['Country of residence'].map(renameCountry).reset_index(drop=True)

    # make a cleaned dataset
    provincial_visitor_count = raw_tourists_in_canada[['REF_DATE', 'GEO', 'Country of residence', 'VALUE']].copy()

    # change column names for legibility
    provincial_visitor_count.columns = ['date', 'destination_province', 'place_of_residence', 'visitor_count']

    print("\n------- Cleaned Dataset 4 Preview -------- ")
    print(provincial_visitor_count.head())

    return provincial_visitor_count