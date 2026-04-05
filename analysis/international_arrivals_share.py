import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os


class QuestionOne:
    def __init__(self) -> None:

        load_dotenv()

        self.engine = create_engine(
            f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('PASSWORD')}@{os.getenv('HOST')}/{os.getenv('DB_NAME')}"
        )

    def answer(self, year_from: int = 1972, year_to: int = 2023, p: str = "month"):
        if p not in ("month", "quarter", "year"):
            raise ValueError(f"p must be one of: month, quarter, year")

        query = text(
            """
            SELECT
                LEFT(date, 7) AS period,
                place_of_residence,
                SUM(visitor_count) AS arrivals
            FROM provincial_visitor_count
            WHERE LEFT(date, 4) BETWEEN :year_from AND :year_to
            GROUP BY period, place_of_residence
            ORDER BY period, arrivals DESC
        """
        )

        with self.engine.connect() as conn:
            df = pd.read_sql(
                query, conn, params={"year_from": year_from, "year_to": year_to}
            )

        if p == "quarter":
            df["period"] = (
                pd.to_datetime(df["period"]).dt.to_period("Q").astype(str)
            )  # eg. 1990Q1
        elif p == "year":
            df["period"] = df["period"].str[:4]  # 1990-01 -> 1990

        if p != "month":
            df = df.groupby(["period", "place_of_residence"], as_index=False)[
                "arrivals"
            ].sum()
        # share: each arrival / sum of all arrivals in that period. transform > groupby().sum()
        df["share"] = df["arrivals"] / df.groupby("period")["arrivals"].transform("sum")
        # period_number: ranks by sequential ints eg 1990-01 < 1990-02
        df["period_number"] = df["period"].rank(method="dense").astype(int)
        df = df.sort_values(["period", "share"], ascending=[True, False]).reset_index(
            drop=True
        )

        return df


if __name__ == "__main__":
    # example
    q = QuestionOne()
    answer = q.answer(year_from=1990, year_to=2000, p="month")
    print(answer.columns)
    print(answer)
    # dirname(__file__) resolves to analysis/, so csv goes there instead of cwd
    answer.to_csv(os.path.join(os.path.dirname(__file__), "Q1.csv"), index=False)
