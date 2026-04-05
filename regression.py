import numpy as np
from scipy import stats
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

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

    slope, intercept, r_value, p_value, std_err = stats.linregress(total_international_tourist_spend, visitor_count_by_month)

    y_predicted = [(slope * x) + intercept for x in total_international_tourist_spend]
    residuals = [actual - pred for actual, pred in zip(visitor_count_by_month, y_predicted)]
    mse = np.mean([r**2 for r in residuals])

    print(f"\nPredicted formula: y = {slope:.4f}x + {intercept:.4f}")
    print(f"Y values: {[y for y in y_predicted[:10]]}")
    print(f"Mean squared error: {mse:.4f}")
    print(f"Residual errors: {[r for r in residuals[:10]]}")

    ax = plt.gca()
    ax.scatter(total_international_tourist_spend, visitor_count_by_month)
    ax.plot(total_international_tourist_spend, y_predicted, color="red")
    plt.title("International Tourist Count vs. Tourist Expenditure by Province")
    plt.xlabel("Spend in Dollars")
    plt.ylabel("Visitor Count")
    plt.show()



def predict_spend_per_arrival(cursor):
    sql = """
    SELECT its.place_of_residence, its.region_visited, SUM(its.amount_spent) / SUM(pvc.visitor_count) AS spend_per_arrival
    FROM international_tourist_spending its JOIN provincial_visitor_count pvc
    ON its.date = pvc.date
    AND pvc.destination_province = CASE
    WHEN its.region_visited LIKE '%British Columbia%' THEN 'British Columbia'
    WHEN its.region_visited LIKE '%Ontario%' THEN 'Ontario'
    WHEN its.region_visited LIKE '%Alberta%' THEN 'Alberta'
    WHEN its.region_visited LIKE '%Quebec%' THEN 'Quebec'
    ELSE its.region_visited
    END
    GROUP BY its.place_of_residence, its.region_visited
    HAVING SUM(pvc.visitor_count) > 0
    """

    cursor.execute(sql)
    rows = cursor.fetchall()
