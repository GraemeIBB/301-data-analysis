import pandas as pd
from scipy import stats

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
    GROUP BY pvc.date, pvc.destination_province LIMIT 10
    """
    
    cursor.execute(sql)
    visitor_count_by_month = []
    total_international_tourist_spend = []

    rows = cursor.fetchall()
    visitor_count_by_month.append(int(row[2]) for row in rows)
    total_international_tourist_spend.append(float(row[3]) for row in rows)

    