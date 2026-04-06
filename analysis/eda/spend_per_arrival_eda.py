import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.patches as mpatches
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'outputs')

LOW_RELIABILITY_PROVINCES = {
    'Prince Edward Island',
    'Nunavut',
    'Northwest Territories',
    'Yukon',
}

EXCLUDE_ORIGINS = {
    'Overseas',
    'Other countries',
}

MIN_QUARTERLY_ARRIVALS = 20

QUERY_SPENDING = text("""
    SELECT date, region_visited, place_of_residence, expenditure_type, amount_spent
    FROM international_tourist_spending
    WHERE region_visited NOT LIKE '%,%'
""")

QUERY_JOINED = text("""
    SELECT
        s.date,
        s.region_visited                                        AS province,
        s.place_of_residence                                    AS origin,
        s.expenditure_type,
        s.amount_spent,
        SUM(v.visitor_count)                                    AS quarterly_arrivals,
        s.amount_spent / NULLIF(SUM(v.visitor_count), 0)       AS spend_per_arrival,
        CEIL(MONTH(STR_TO_DATE(s.date, '%Y-%m-%d')) / 3)       AS quarter,
        LEFT(s.date, 4)                                         AS year
    FROM international_tourist_spending s
    LEFT JOIN provincial_visitor_count v
        ON  s.region_visited = v.destination_province
        AND s.place_of_residence = v.place_of_residence
        AND LEFT(s.date, 4) = LEFT(v.date, 4)
        AND CEIL(MONTH(STR_TO_DATE(v.date, '%Y-%m')) / 3) =
            CEIL(MONTH(STR_TO_DATE(s.date, '%Y-%m-%d')) / 3)
    WHERE s.region_visited NOT LIKE '%,%'
    GROUP BY
        s.date, s.region_visited, s.place_of_residence,
        s.expenditure_type, s.amount_spent
""")


def _save(fig, filename):
    fig.savefig(os.path.join(OUTPUT_DIR, filename), dpi=150, bbox_inches='tight')
    plt.close(fig)


def _connect():
    load_dotenv()
    return create_engine(
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('PASSWORD')}"
        f"@{os.getenv('HOST')}/{os.getenv('DB_NAME')}"
    )


def _apply_local_fixes(df, origin_col):
    df[origin_col] = df[origin_col].replace({'South Korea': 'Korea, South'})
    return df


def run_eda():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    engine = _connect()
    with engine.connect() as conn:
        df_spending = pd.read_sql(QUERY_SPENDING, conn)
        df_joined = pd.read_sql(QUERY_JOINED, conn)

    df_spending = _apply_local_fixes(df_spending, 'place_of_residence')
    df_joined = _apply_local_fixes(df_joined, 'origin')

    df_joined['year'] = df_joined['year'].astype(int)
    df_joined['covid_period'] = df_joined['year'].between(2020, 2022)

    # 1. Missingness report (raw tables, pre-cleaning)
    null_spending = pd.DataFrame({
        'column': df_spending.columns,
        'null_count': df_spending.isnull().sum().values,
        'null_pct': (100 * df_spending.isnull().mean()).round(1).values
    })
    null_joined = pd.DataFrame({
        'column': df_joined.columns,
        'null_count': df_joined.isnull().sum().values,
        'null_pct': (100 * df_joined.isnull().mean()).round(1).values
    })

    null_report = pd.concat(
        [null_spending.assign(table='international_tourist_spending'),
        null_joined.assign(table='joined')],
        ignore_index=True
    )

    null_report.to_csv(os.path.join(OUTPUT_DIR, 'eda_null_report.csv'), index=False)
    zero_spend = df_joined[
        (df_joined['quarterly_arrivals'].notna()) & 
        (df_joined['amount_spent'] == 0)
    ]

    # 2. Coverage report
    total_rows = len(df_spending)

    excluded_origin_rows = df_spending[
        df_spending['place_of_residence'].isin(EXCLUDE_ORIGINS)
    ].shape[0]

    after_origin_exclusion = df_joined[~df_joined['origin'].isin(EXCLUDE_ORIGINS)]

    after_null_arrivals = after_origin_exclusion[
        after_origin_exclusion['quarterly_arrivals'].notna() &
        (after_origin_exclusion['quarterly_arrivals'] > 0)
    ]

    df_usable = after_null_arrivals[
        after_null_arrivals['quarterly_arrivals'] >= MIN_QUARTERLY_ARRIVALS
    ]

    coverage = pd.DataFrame({
        'stage': [
            'Province-level spending rows (sub-regions excluded — confirmed duplicates)',
            'After excluding Overseas and Other countries (aggregate origin buckets)',
            'After excluding NULL or zero arrivals (suppressed visitor counts)',
            f'After excluding < {MIN_QUARTERLY_ARRIVALS} quarterly arrivals (sparse pairs)',
        ],
        'row_count': [
            total_rows,
            total_rows - excluded_origin_rows,
            len(after_null_arrivals),
            len(df_usable),
        ]
    })
    coverage['pct_of_province_rows'] = (
        100.0 * coverage['row_count'] / total_rows
    ).round(1)

    coverage.to_csv(os.path.join(OUTPUT_DIR, 'eda_spend_coverage.csv'), index=False)

    # 3. Summary statistics
    summary = pd.DataFrame({
        'amount_spent':       df_usable['amount_spent'].describe(),
        'spend_per_arrival':  df_usable['spend_per_arrival'].describe(),
        'quarterly_arrivals': df_usable['quarterly_arrivals'].describe(),
    })

    summary.to_csv(os.path.join(OUTPUT_DIR, 'eda_spend_summary_stats.csv'))

    pre_covid = df_usable[~df_usable['covid_period']]

    # 4. Distribution of amount_spent
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))

    for ax, log, title in zip(
        axes,
        [False, True],
        ['Distribution of Amount Spent (raw)', 'Distribution of Amount Spent (log scale)']
    ):
        ax.hist(df_usable['amount_spent'], bins=60,
                color='steelblue', edgecolor='none', log=log)
        ax.xaxis.set_major_formatter(
            mtick.FuncFormatter(lambda x, _: f'${x/1e6:.0f}M')
        )
        ax.set_title(title)
        ax.set_xlabel('Amount Spent')
        ax.set_ylabel('Observations (log scale)' if log else 'Observations')

    fig.suptitle('Spending Distribution — international_tourist_spending')
    plt.tight_layout()
    _save(fig, 'eda_spend_distribution.png')

    # 5. Total spending over time by expenditure type
    spending_over_time = (
        df_usable
        .groupby(['year', 'expenditure_type'])['amount_spent']
        .sum()
        .reset_index()
    )
    pivot_time = spending_over_time.pivot(
        index='year', columns='expenditure_type', values='amount_spent'
    ).fillna(0)

    fig, ax = plt.subplots(figsize=(13, 6))
    pivot_time.plot(ax=ax, marker='o', markersize=4)
    ax.axvspan(2020, 2022, alpha=0.15, color='red', label='COVID-19 period (2020–2022)')
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'${x/1e9:.1f}B'))
    ax.set_xlabel('Year')
    ax.set_ylabel('Total Spend')
    ax.set_title('International Tourist Spending Over Time by Expenditure Type')
    ax.legend(fontsize=8)
    ax.grid(axis='y', linestyle='--', alpha=0.4)
    _save(fig, 'eda_spend_over_time.png')

    # 6. Average spend per arrival by origin country (pre-COVID baseline)
    spend_by_origin = (
        pre_covid
        .groupby('origin')
        .agg(
            total_spend=('amount_spent', 'sum'),
            total_arrivals=('quarterly_arrivals', 'sum')
        )
        .reset_index()
    )
    spend_by_origin['spend_per_arrival'] = (
        spend_by_origin['total_spend'] / spend_by_origin['total_arrivals']
    )
    spend_by_origin = spend_by_origin.sort_values('spend_per_arrival')

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(
        spend_by_origin['origin'],
        spend_by_origin['spend_per_arrival'],
        color='steelblue'
    )
    for bar in bars:
        width = bar.get_width()
        ax.text(width * 1.01, bar.get_y() + bar.get_height() / 2,
                f'${width:,.0f}', va='center', fontsize=8)

    ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'${x:,.0f}'))
    ax.set_xlabel('Average Spend per Arrival (CAD)')
    ax.set_title('Average Spend per Arrival by Origin Country (pre-COVID, all provinces)')
    ax.grid(axis='x', linestyle='--', alpha=0.4)
    _save(fig, 'eda_spend_per_arrival_by_origin.png')

    # 7. Spend per arrival by season and origin (pre-COVID baseline)
    spend_by_season = (
        pre_covid
        .groupby(['quarter', 'origin'])
        .agg(
            total_spend=('amount_spent', 'sum'),
            total_arrivals=('quarterly_arrivals', 'sum')
        )
        .reset_index()
    )
    spend_by_season['spend_per_arrival'] = (
        spend_by_season['total_spend'] / spend_by_season['total_arrivals']
    )

    pivot_season = spend_by_season.pivot(
        index='origin', columns='quarter', values='spend_per_arrival'
    )
    pivot_season.columns = ['Q1 (Jan–Mar)', 'Q2 (Apr–Jun)',
                            'Q3 (Jul–Sep)', 'Q4 (Oct–Dec)']

    fig, ax = plt.subplots(figsize=(12, 6))
    pivot_season.plot(kind='bar', ax=ax, width=0.75)
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'${x:,.0f}'))
    ax.set_xlabel('Origin Country')
    ax.set_ylabel('Average Spend per Arrival (CAD)')
    ax.set_title('Spend per Arrival by Origin Country and Season (pre-COVID)')
    ax.legend(title='Quarter', fontsize=8)
    ax.tick_params(axis='x', rotation=45)
    ax.grid(axis='y', linestyle='--', alpha=0.4)
    plt.tight_layout()
    _save(fig, 'eda_spend_per_arrival_seasonality.png')

    # 8. Spend per arrival by province — reliability flagged (pre-COVID)
    spend_by_province = (
        pre_covid
        .groupby('province')
        .agg(
            total_spend=('amount_spent', 'sum'),
            total_arrivals=('quarterly_arrivals', 'sum')
        )
        .reset_index()
    )
    spend_by_province['spend_per_arrival'] = (
        spend_by_province['total_spend'] / spend_by_province['total_arrivals']
    )
    spend_by_province['reliable'] = ~spend_by_province['province'].isin(
        LOW_RELIABILITY_PROVINCES
    )
    spend_by_province = spend_by_province.sort_values('spend_per_arrival')

    colors = spend_by_province['reliable'].map(
        {True: 'steelblue', False: 'tomato'}
    )

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(
        spend_by_province['province'],
        spend_by_province['spend_per_arrival'],
        color=colors
    )
    for bar in ax.patches:
        width = bar.get_width()
        ax.text(width * 1.01, bar.get_y() + bar.get_height() / 2,
                f'${width:,.0f}', va='center', fontsize=8)

    ax.legend(handles=[
        mpatches.Patch(color='steelblue', label='Reliable'),
        mpatches.Patch(color='tomato',    label='Low reliability (flag in report)'),
    ], fontsize=8)
    ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'${x:,.0f}'))
    ax.set_xlabel('Average Spend per Arrival (CAD)')
    ax.set_title('Spend per Arrival by Province — Reliability Flagged (pre-COVID)')
    ax.grid(axis='x', linestyle='--', alpha=0.4)
    _save(fig, 'eda_spend_per_arrival_reliability.png')

    # 9. Arrival volume vs spend per arrival
    volume_vs_spend = (
        pre_covid[~pre_covid['province'].isin(LOW_RELIABILITY_PROVINCES)]
        .groupby(['origin', 'province'])
        .agg(
            total_spend=('amount_spent', 'sum'),
            total_arrivals=('quarterly_arrivals', 'sum')
        )
        .reset_index()
    )
    volume_vs_spend['spend_per_arrival'] = (
        volume_vs_spend['total_spend'] / volume_vs_spend['total_arrivals']
    )

    fig, ax = plt.subplots(figsize=(11, 6))
    colors_scatter = plt.cm.tab10.colors

    for i, origin in enumerate(sorted(volume_vs_spend['origin'].unique())):
        subset = volume_vs_spend[volume_vs_spend['origin'] == origin]
        ax.scatter(
            subset['total_arrivals'],
            subset['spend_per_arrival'],
            label=origin,
            color=colors_scatter[i % len(colors_scatter)],
            alpha=0.75,
            s=50
        )

    ax.set_xscale('log')
    ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{x/1e3:.0f}K'))
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'${x:,.0f}'))
    ax.set_xlabel('Total Quarterly Arrivals (log scale)')
    ax.set_ylabel('Spend per Arrival (CAD)')
    ax.set_title(
        'Arrival Volume vs Spend per Arrival by Origin and Province\n'
        '(pre-COVID, reliable provinces only)'
    )
    ax.legend(fontsize=7, ncol=2)
    ax.grid(linestyle='--', alpha=0.4)
    plt.tight_layout()
    _save(fig, 'eda_volume_vs_spend.png')

    print("EDA completed. Outputs saved to: ", OUTPUT_DIR)

if __name__ == "__main__":
    run_eda()