import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt

def predict_quarterly_international_tourist_arrivals(cursor):
    sql = """
    SELECT pvc.date, pvc.destination_province, SUM(pvc.visitor_count) AS visitor_count_by_month,
    SUM(its.amount_spent) as total_international_tourist_spend
    FROM provincial_visitor_count pvc JOIN international_tourist_spending its
    ON pvc.date = its.date
    AND pvc.destination_province = CASE WHEN its.region_visited LIKE '%British Columbia%' THEN 'British Columbia'
    WHEN its.region_visited LIKE '%Ontario%' THEN 'Ontario'
    WHEN its.region_visited LIKE '%Alberta%' THEN 'Alberta'
    WHEN its.region_visited LIKE '%Quebec%' THEN 'Quebec'
    ELSE its.region_visited
    END
    GROUP BY pvc.date, pvc.destination_province
    """
    
    cursor.execute(sql)

    rows = cursor.fetchall()
    visitor_count_by_month = [int(row[2]) for row in rows]
    total_international_tourist_spend = [float(row[3]) for row in rows]

    slope, intercept = stats.linregress(total_international_tourist_spend, visitor_count_by_month)

    y_predicted = [(slope * x) + intercept for x in total_international_tourist_spend]
    residuals = [actual - pred for actual, pred in zip(visitor_count_by_month, y_predicted)]
    mse = np.mean([r**2 for r in residuals])

    print(f"\nPredicted formula: y = {slope}x + {intercept}")
    print(f"Y values: {[y for y in y_predicted[:10]]}")
    print(f"Prediction Error: {mse}")
    print(f"Residual errors: {[r for r in residuals[:10]]}")

    ax = plt.gca()
    ax.scatter(total_international_tourist_spend, visitor_count_by_month)
    ax.plot(total_international_tourist_spend, y_predicted, color="red")
    plt.title("International Tourist Arrivals by Province per Month")
    plt.xlabel("Total International Tourist Spend by Province per Month (Dollars)")
    plt.ylabel("International Visitor Count by Province per Month")
    plt.show()