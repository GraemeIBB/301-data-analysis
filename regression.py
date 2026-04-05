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
    print(f"R^2 value: {r_value**2:.4f}")
    print(f"P value: {p_value:.4f}")
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
    SELECT SUM(its.amount_spent) / SUM(pvc.visitor_count) AS spend_per_arrival, SUM(pvc.visitor_count) AS total_arrivals
    FROM international_tourist_spending its JOIN provincial_visitor_count pvc
    ON its.date = pvc.date
    AND pvc.destination_province = CASE WHEN its.region_visited LIKE '%British Columbia%' THEN 'British Columbia'
    WHEN its.region_visited LIKE '%Ontario%' THEN 'Ontario'
    WHEN its.region_visited LIKE '%Alberta%' THEN 'Alberta'
    WHEN its.region_visited LIKE '%Quebec%' THEN 'Quebec'
    ELSE its.region_visited
    END
    GROUP BY its.date, its.region_visited
    HAVING SUM(pvc.visitor_count) > 0
    """

    cursor.execute(sql)
    rows = cursor.fetchall()

    spend_per_arrival = [float(row[0]) for row in rows]
    total_arrivals = [int(row[1]) for row in rows]

    slope, intercept, r_value, p_value, std_err = stats.linregress(total_arrivals, spend_per_arrival)

    y_predicted = [(slope * x) + intercept for x in total_arrivals]
    residuals = [actual - pred for actual, pred in zip(spend_per_arrival, y_predicted)]
    mse = np.mean([r**2 for r in residuals])

    print(f"\nPredicted formula: y = {slope:.4f}x + {intercept:.4f}")
    print(f"R^2 value: {r_value**2:.4f}")
    print(f"P value: {p_value:.4f}")
    print(f"Y values: {[y for y in y_predicted[:10]]}")
    print(f"Mean squared error: {mse:.4f}")
    print(f"Residual errors: {[r for r in residuals[:10]]}")

    ax = plt.gca()
    ax.scatter(total_arrivals, spend_per_arrival)
    ax.plot(total_arrivals, y_predicted, color="red")
    plt.title("Spend per Arrival vs. Total Arrivals")
    plt.xlabel("Total Arrivals")
    plt.ylabel("Spend per Arrival in Dollars")
    plt.show()