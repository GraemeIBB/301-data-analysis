import numpy as np
import pandas as pd
from scipy import stats
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder
import matplotlib.pyplot as plt


def predict_spend_per_arrival(cursor):
    sql = """
    SELECT
        its.date,
        CASE WHEN its.region_visited LIKE '%British Columbia%' THEN 'British Columbia'
             WHEN its.region_visited LIKE '%Ontario%' THEN 'Ontario'
             WHEN its.region_visited LIKE '%Alberta%' THEN 'Alberta'
             WHEN its.region_visited LIKE '%Quebec%' THEN 'Quebec'
             ELSE its.region_visited
        END AS province,
        its.place_of_residence AS source_country,
        SUM(its.amount_spent) / SUM(pvc.visitor_count) AS spend_per_arrival
    FROM international_tourist_spending its
    JOIN provincial_visitor_count pvc
        ON its.date = pvc.date
        AND its.place_of_residence = pvc.place_of_residence
        AND pvc.destination_province = CASE WHEN its.region_visited LIKE '%British Columbia%' THEN 'British Columbia'
             WHEN its.region_visited LIKE '%Ontario%' THEN 'Ontario'
             WHEN its.region_visited LIKE '%Alberta%' THEN 'Alberta'
             WHEN its.region_visited LIKE '%Quebec%' THEN 'Quebec'
             ELSE its.region_visited
        END
    WHERE its.place_of_residence NOT IN ('Other countries', 'Overseas')
    GROUP BY its.date, province, its.place_of_residence
    HAVING SUM(pvc.visitor_count) > 0
    """

    cursor.execute(sql)
    rows = cursor.fetchall()

    df = pd.DataFrame(rows, columns=['date', 'province', 'source_country', 'spend_per_arrival'])
    df['spend_per_arrival'] = df['spend_per_arrival'].astype(float)

    def get_season(date_str):
        month = int(date_str.split('-')[1])
        if month <= 3: return 'Winter'
        elif month <= 6: return 'Spring'
        elif month <= 9: return 'Summer'
        else: return 'Fall'

    df['season'] = df['date'].apply(get_season)

    encoder = OneHotEncoder(sparse_output=False, drop='first')
    X = encoder.fit_transform(df[['source_country', 'province', 'season']])
    y = df['spend_per_arrival'].to_numpy(dtype=float)

    model = LinearRegression()
    model.fit(X, y)

    y_predicted = model.predict(X)
    residuals = y - y_predicted
    mse = np.mean(residuals ** 2)
    r_squared = model.score(X, y)

    n, p = X.shape
    ss_res = np.sum(residuals ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    f_stat = ((ss_tot - ss_res) / p) / (ss_res / (n - p - 1))
    p_value = 1 - stats.f.cdf(f_stat, p, n - p - 1)

    print(f"\nR^2 value: {r_squared:.4f}")
    print(f"F-statistic: {f_stat:.4f}")
    print(f"P value: {p_value:.6f}")
    print(f"Mean squared error: {mse:.4f}")
    print(f"Sample size: {n}")

    ax = plt.gca()
    ax.scatter(y, y_predicted, alpha=0.5)
    ax.plot([y.min(), y.max()], [y.min(), y.max()], color='red', linestyle='--', label='Perfect fit')
    plt.title("Spend per Arrival: Actual vs Predicted\n(Source Country + Province + Season)")
    plt.xlabel("Actual Spend per Arrival ($)")
    plt.ylabel("Predicted Spend per Arrival ($)")
    plt.legend()
    plt.show()
