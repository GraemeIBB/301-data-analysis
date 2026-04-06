import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'outputs')

LOW_RELIABILITY_PROVINCES = {
    'Prince Edward Island', 'Nunavut', 'Northwest Territories', 'Yukon',
}
EXCLUDE_ORIGINS    = {'Overseas', 'Other countries'}
MIN_ARRIVALS       = 20
PRE_COVID_YEARS    = (2018, 2019)
POST_COVID_YEARS   = (2022, 2023)


class QuestionTwo:
    """
    Q2: How does spend per arrival vary across source countries, provinces,
    and seasons, and how has this relationship shifted over time?

    Outputs (Tableau inputs):
        spend_per_arrival.csv  — full time series, one row per province/origin/quarter
        spend_per_arrival_shift.csv — pre vs post COVID comparison per province/origin
        
    Known limitations are documented in the eda summary. 
    """

    def __init__(self):
        load_dotenv()
        self.engine = create_engine(
            f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('PASSWORD')}"
            f"@{os.getenv('HOST')}/{os.getenv('DB_NAME')}"
        )

    def _fetch(self, year_from: int, year_to: int) -> pd.DataFrame:
        """Core join — returns one row per province/origin/quarter with spend and arrivals."""
        query = text("""
            SELECT
                LEFT(s.date, 4)                                     AS year,
                CEIL(MONTH(STR_TO_DATE(s.date, '%Y-%m-%d')) / 3)    AS quarter,
                s.region_visited                                     AS province,
                s.place_of_residence                                 AS origin,
                SUM(s.amount_spent)                                  AS total_spend,
                SUM(v.visitor_count)                                 AS quarterly_arrivals
            FROM international_tourist_spending s
            LEFT JOIN provincial_visitor_count v
                ON  s.region_visited   = v.destination_province
                AND s.place_of_residence = v.place_of_residence
                AND LEFT(s.date, 4)    = LEFT(v.date, 4)
                AND CEIL(MONTH(STR_TO_DATE(v.date,   '%Y-%m'))   / 3) =
                    CEIL(MONTH(STR_TO_DATE(s.date, '%Y-%m-%d')) / 3)
            WHERE s.region_visited NOT LIKE '%,%'
              AND LEFT(s.date, 4) BETWEEN :y0 AND :y1
            GROUP BY LEFT(s.date, 4), CEIL(MONTH(STR_TO_DATE(s.date, '%Y-%m-%d')) / 3),
                     s.region_visited, s.place_of_residence
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={'y0': year_from, 'y1': year_to})

        df['origin'] = df['origin'].replace({'South Korea': 'Korea, South'})
        df = df[~df['origin'].isin(EXCLUDE_ORIGINS)]
        df = df[df['quarterly_arrivals'].fillna(0) >= MIN_ARRIVALS].reset_index(drop=True)

        df['year']            = df['year'].astype(int)
        df['quarter']         = df['quarter'].astype(int)
        df['spend_per_arrival'] = df['total_spend'] / df['quarterly_arrivals']
        df['period']          = df['year'].astype(str) + '-Q' + df['quarter'].astype(str)
        df['covid_period']    = df['year'].between(2020, 2022)
        df['low_reliability'] = df['province'].isin(LOW_RELIABILITY_PROVINCES)
        return df

    def answer(self, year_from: int = 2018, year_to: int = 2023) -> pd.DataFrame:
        """
        Full quarterly time series of spend per arrival.
        Primary Tableau input — connect to spend_per_arrival.csv.
        """
        df = self._fetch(year_from, year_to)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out = os.path.join(OUTPUT_DIR, 'spend_per_arrival.csv')
        df.to_csv(out, index=False)
        print(f"spend_per_arrival.csv written — {len(df)} rows")
        return df

    def shift(self) -> pd.DataFrame:
        """
        Pre vs post COVID comparison of spend per arrival per province/origin.
        Note: post-COVID visitor counts are suppressed for all non-US origins
        by StatCan, so meaningful shift results are limited to United States.
        All other origins will appear as NaN in post_covid columns.
        """
        df = self._fetch(PRE_COVID_YEARS[0], POST_COVID_YEARS[1])
        df = df[~df['covid_period']]

        def _agg(years):
            return (
                df[df['year'].between(*years)]
                .groupby(['province', 'origin', 'low_reliability'], as_index=False)
                .apply(lambda g: pd.Series({
                    'spend_per_arrival': g['total_spend'].sum() / g['quarterly_arrivals'].sum()
                }), include_groups=False)
            )

        pre  = _agg(PRE_COVID_YEARS).rename(columns={'spend_per_arrival': 'pre_covid'})
        post = _agg(POST_COVID_YEARS).rename(columns={'spend_per_arrival': 'post_covid'})

        result = pre.merge(post, on=['province', 'origin', 'low_reliability'], how='outer')
        result['absolute_change'] = result['post_covid'] - result['pre_covid']
        result['pct_change']      = (100 * result['absolute_change'] / result['pre_covid']).round(2)

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out = os.path.join(OUTPUT_DIR, 'spend_per_arrival_shift.csv')
        result.to_csv(out, index=False)
        print(f"spend_per_arrival_shift.csv written — {len(result)} rows")
        return result


if __name__ == "__main__":
    q = QuestionTwo()
    q.answer()
    q.shift()