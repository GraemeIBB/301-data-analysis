import pandas as pd
    
interprov_expenditure = pd.read_csv("data/interprovincial_tourism_expenditures/24100044.csv")
foreign_spending = pd.read_csv("data/spending_by_foreign_tourists/24100047.csv")
tourism_expenditure = pd.read_csv("data/tourism_supply_expenditure/24100004.csv")
tourists_in_canada = pd.read_csv("data/tourists_entering_canada/24100050.csv", dtype={'STATUS': str, 'TERMINATED': str})

