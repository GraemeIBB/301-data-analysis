import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os


OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")

EXCLUDE_ORIGINS = {"Overseas", "Other countries"}
LOW_VOLUME_THRESHOLD = 20
PRE_COVID_YEARS = (2015, 2019)
POST_COVID_YEARS = (2022, 2023)


class QuestionOne:
    """
    Q1: How has the share of total international arrivals attributable to each
    source country and region changed over time?

    Outputs (Tableau inputs):
        arrivals_share_yearly.csv    — yearly time series, one row per origin/year
        arrivals_share_quarterly.csv — quarterly time series, one row per origin/quarter
        arrivals_share_shift.csv     — pre vs post COVID mean share comparison per origin
    """

    def __init__(self) -> None:
        load_dotenv()
        self.engine = create_engine(
            f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('PASSWORD')}@{os.getenv('HOST')}/{os.getenv('DB_NAME')}"
        )

    def answer(
        self, year_from: int = 1972, year_to: int = 2023, p: str = "month"
    ) -> pd.DataFrame:
        if p not in ("month", "quarter", "year"):
            raise ValueError(f"p must be one of: month, quarter, year")

        query = text(
            """
            SELECT
                LEFT(date, 7)      AS period,
                place_of_residence AS origin,
                SUM(visitor_count) AS arrivals
            FROM provincial_visitor_count
            WHERE LEFT(date, 4) BETWEEN :year_from AND :year_to
            GROUP BY period, origin
            ORDER BY period, arrivals DESC
        """
        )

        with self.engine.connect() as conn:
            df = pd.read_sql(
                query, conn, params={"year_from": year_from, "year_to": year_to}
            )

        if p == "quarter":
            df["period"] = pd.to_datetime(df["period"]).dt.to_period("Q").astype(str)
        elif p == "year":
            df["period"] = df["period"].str[:4]

        if p != "month":
            df = df.groupby(["period", "origin"], as_index=False)["arrivals"].sum()

        df["share"] = df["arrivals"] / df.groupby("period")["arrivals"].transform("sum")
        df["period_number"] = df["period"].rank(method="dense").astype(int)
        df = df.sort_values(["period", "share"], ascending=[True, False]).reset_index(
            drop=True
        )

        return df

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove aggregate buckets and StatCan-suppressed low-volume origins."""
        df = df[~df["origin"].isin(EXCLUDE_ORIGINS)].copy()
        peak = df.groupby("origin")["arrivals"].max()
        reliable = peak[peak >= LOW_VOLUME_THRESHOLD].index
        return df[df["origin"].isin(reliable)].copy()

    def export(self) -> None:
        """
        Write Tableau-ready CSVs for yearly and quarterly granularities.
        Share is recomputed after exclusions so it sums to 1 within each period.
        """
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        for p, label in [("year", "yearly"), ("quarter", "quarterly")]:
            df = self._clean(self.answer(p=p))

            # Recompute share after exclusions
            group_cols = ["period"]
            df["share"] = df["arrivals"] / df.groupby(group_cols)["arrivals"].transform(
                "sum"
            )
            df["year"] = df["period"].str[:4].astype(int)
            if p == "quarter":
                df["quarter"] = df["period"].str[-1].astype(int)
            df["covid_period"] = df["year"].between(2020, 2022)
            df["rank"] = (
                df.groupby("period")["arrivals"]
                .rank(ascending=False, method="min")
                .astype(int)
            )

            out = os.path.join(OUTPUT_DIR, f"arrivals_share_{label}.csv")
            df.to_csv(out, index=False)
            print(f"arrivals_share_{label}.csv written — {len(df)} rows")

    def shift(self) -> pd.DataFrame:
        """
        Pre vs post COVID mean share comparison per origin.
        Tableau input — connect to arrivals_share_shift.csv.
        """
        df = self._clean(self.answer(p="year"))
        df["year"] = df["period"].astype(int)
        df["share"] = df["arrivals"] / df.groupby("period")["arrivals"].transform("sum")
        df = df[~df["year"].between(2020, 2022)]

        def _mean_share(years):
            return df[df["year"].between(*years)].groupby("origin")["share"].mean()

        pre = _mean_share(PRE_COVID_YEARS).rename("pre_covid_share")
        post = _mean_share(POST_COVID_YEARS).rename("post_covid_share")

        result = pd.concat([pre, post], axis=1).reset_index()
        result["absolute_change"] = (
            result["post_covid_share"] - result["pre_covid_share"]
        )
        result["pct_change"] = (
            100 * result["absolute_change"] / result["pre_covid_share"]
        ).round(2)

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out = os.path.join(OUTPUT_DIR, "arrivals_share_shift.csv")
        result.to_csv(out, index=False)
        print(f"arrivals_share_shift.csv written — {len(result)} rows")
        return result


if __name__ == "__main__":
    q = QuestionOne()
    q.export()
    q.shift()
