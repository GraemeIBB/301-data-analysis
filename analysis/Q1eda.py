# How has the share of total international arrivals attributable to each source country and region changed over time?
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from sqlalchemy import text
from share_calc import QuestionOne

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'eda', 'outputs', 'q1_eda')

# Aggregate buckets - not real countries, exclude from share analysis
EXCLUDE_ORIGINS = {'Overseas', 'Other countries'}

# Drop origins whose peak annual arrivals never reach this - StatCan suppression noise
LOW_VOLUME_THRESHOLD = 500


def _save(fig, filename):
    fig.savefig(os.path.join(OUTPUT_DIR, filename), dpi=150, bbox_inches='tight')
    plt.close(fig)


def run_eda():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    q1 = QuestionOne()

    # 1. Missingness report (raw table, via q1.engine)
    with q1.engine.connect() as conn:
        df_raw = pd.read_sql(
            text("SELECT date, destination_province, place_of_residence, visitor_count FROM provincial_visitor_count"),
            conn,
        )

    null_report = pd.DataFrame({
        'column':     df_raw.columns,
        'null_count': df_raw.isnull().sum().values,
        'null_pct':   (100 * df_raw.isnull().mean()).round(1).values,
    })
    null_report.to_csv(os.path.join(OUTPUT_DIR, 'q1_null_report.csv'), index=False)

    # 2-10. Share analysis via QuestionOne
    df_year = q1.answer(p='year')
    df_year['covid_period'] = df_year['year'].between(2020, 2022)

    df_quarter = q1.answer(p='quarter')

    # Apply exclusions and low-volume filter
    def _clean(df):
        df = df[~df['origin'].isin(EXCLUDE_ORIGINS)].copy()
        peak = df.groupby('origin')['arrivals'].max()
        reliable = peak[peak >= LOW_VOLUME_THRESHOLD].index
        return df[df['origin'].isin(reliable)]

    df_year = _clean(df_year)
    df_quarter = _clean(df_quarter)

    # Recompute share after exclusions
    df_year['share'] = df_year['arrivals'] / df_year.groupby('year')['arrivals'].transform('sum')
    df_quarter['share'] = df_quarter['arrivals'] / df_quarter.groupby(['year', 'quarter'])['arrivals'].transform('sum')

    pre_covid = df_year[~df_year['covid_period']]
    top_origins = (
        pre_covid.groupby('origin')['share'].mean()
        .sort_values(ascending=False)
        .head(10)
        .index.tolist()
    )

    # 2. Coverage report
    coverage = pd.DataFrame({
        'stage': [
            'After excluding aggregate buckets (Overseas, Other countries)',
            f'After excluding origins with peak annual arrivals < {LOW_VOLUME_THRESHOLD}',
        ],
        'distinct_origins': [
            df_year['origin'].nunique() + (df_year['origin'].isin(EXCLUDE_ORIGINS)).sum(),
            df_year['origin'].nunique(),
        ],
    })
    coverage.to_csv(os.path.join(OUTPUT_DIR, 'q1_coverage.csv'), index=False)

    # 3. Summary statistics
    df_year['arrivals'].describe().to_frame('arrivals').to_csv(
        os.path.join(OUTPUT_DIR, 'q1_summary_stats.csv')
    )

    # 4. Distribution of annual arrivals (raw + log)
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    for ax, log, title in zip(
        axes,
        [False, True],
        ['Annual Arrivals per Origin (raw)', 'Annual Arrivals per Origin (log scale)'],
    ):
        ax.hist(df_year['arrivals'], bins=60, color='steelblue', edgecolor='none', log=log)
        ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{x/1e3:.0f}K'))
        ax.set_title(title)
        ax.set_xlabel('Annual Arrivals')
        ax.set_ylabel('Observations (log)' if log else 'Observations')
    fig.suptitle('Distribution of Annual Arrivals - provincial_visitor_count')
    plt.tight_layout()
    _save(fig, 'q1_arrivals_distribution.png')

    # 5. Total arrivals over time
    total_by_year = df_year.groupby('year')['arrivals'].sum().reset_index()

    fig, ax = plt.subplots(figsize=(13, 5))
    ax.plot(total_by_year['year'], total_by_year['arrivals'], marker='o', markersize=4, color='steelblue')
    ax.axvspan(2020, 2022, alpha=0.15, color='red', label='COVID-19 period (2020-2022)')
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{x/1e6:.0f}M'))
    ax.set_xlabel('Year')
    ax.set_ylabel('Total Arrivals')
    ax.set_title('Total International Arrivals Over Time (all reliable origins)')
    ax.legend()
    ax.grid(axis='y', linestyle='--', alpha=0.4)
    _save(fig, 'q1_total_arrivals_over_time.png')

    # 6. Share over time - top 10 origins
    pivot_share = (
        df_year[df_year['origin'].isin(top_origins)]
        .pivot(index='year', columns='origin', values='share')
    )

    fig, ax = plt.subplots(figsize=(14, 7))
    for col in pivot_share.columns:
        ax.plot(pivot_share.index, pivot_share[col], marker='o', markersize=3, label=col)
    ax.axvspan(2020, 2022, alpha=0.12, color='red', label='COVID-19 period')
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{x:.0%}'))
    ax.set_xlabel('Year')
    ax.set_ylabel('Share of Total Arrivals')
    ax.set_title('Share of International Arrivals by Source Country - Top 10 Origins')
    ax.legend(fontsize=8, ncol=2)
    ax.grid(axis='y', linestyle='--', alpha=0.4)
    plt.tight_layout()
    _save(fig, 'q1_share_over_time_top10.png')

    # 7. US vs non-US share split over time
    us_share = df_year[df_year['origin'] == 'United States'].set_index('year')['share']
    non_us_share = 1 - us_share

    fig, ax = plt.subplots(figsize=(13, 5))
    ax.stackplot(
        us_share.index,
        us_share.values,
        non_us_share.values,
        labels=['United States', 'All other origins'],
        colors=['steelblue', 'lightcoral'],
        alpha=0.8,
    )
    ax.axvspan(2020, 2022, alpha=0.15, color='red', label='COVID-19 period (2020-2022)')
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{x:.0%}'))
    ax.set_xlabel('Year')
    ax.set_ylabel('Share of Total Arrivals')
    ax.set_title('US vs. Non-US Share of International Arrivals Over Time')
    ax.legend(loc='upper right', fontsize=9)
    ax.grid(axis='y', linestyle='--', alpha=0.4)
    _save(fig, 'q1_us_vs_nonus_share.png')

    # 8. Pre-COVID vs post-COVID mean share - top 10
    post_covid = df_year[df_year['year'].between(2022, 2023)]
    pre_mean = pre_covid[pre_covid['origin'].isin(top_origins)].groupby('origin')['share'].mean()
    post_mean = post_covid[post_covid['origin'].isin(top_origins)].groupby('origin')['share'].mean()

    compare = pd.DataFrame({'pre_covid': pre_mean, 'post_covid': post_mean}).dropna().sort_values('pre_covid')

    fig, ax = plt.subplots(figsize=(10, 6))
    x = range(len(compare))
    ax.barh([i - 0.2 for i in x], compare['pre_covid'],  height=0.38, label='Pre-COVID mean',             color='steelblue')
    ax.barh([i + 0.2 for i in x], compare['post_covid'], height=0.38, label='Post-COVID mean (2022-2023)', color='lightcoral')
    ax.set_yticks(list(x))
    ax.set_yticklabels(compare.index)
    ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{x:.1%}'))
    ax.set_xlabel('Mean Share of Total Arrivals')
    ax.set_title('Mean Arrival Share: Pre-COVID vs. Post-COVID - Top 10 Origins')
    ax.legend()
    ax.grid(axis='x', linestyle='--', alpha=0.4)
    plt.tight_layout()
    _save(fig, 'q1_share_pre_vs_post_covid.png')

    # 9. Rank stability over time
    df_year['rank'] = df_year.groupby('year')['arrivals'].rank(ascending=False, method='min')

    rank_pivot = (
        df_year[df_year['origin'].isin(top_origins)]
        .pivot(index='year', columns='origin', values='rank')
    )

    fig, ax = plt.subplots(figsize=(14, 6))
    for col in rank_pivot.columns:
        ax.plot(rank_pivot.index, rank_pivot[col], marker='o', markersize=3, label=col)
    ax.axvspan(2020, 2022, alpha=0.12, color='red', label='COVID-19 period')
    ax.invert_yaxis()
    ax.set_xlabel('Year')
    ax.set_ylabel('Arrival Rank (1 = highest)')
    ax.set_title('Arrival Rank Over Time - Top 10 Origins')
    ax.legend(fontsize=8, ncol=2)
    ax.grid(axis='y', linestyle='--', alpha=0.4)
    plt.tight_layout()
    _save(fig, 'q1_rank_stability.png')

    # 10. Quarterly seasonality of share - pre-COVID
    pre_covid_q = df_quarter[
        (df_quarter['year'] < 2020) & (df_quarter['origin'].isin(top_origins))
    ]

    season_mean = (
        pre_covid_q.groupby(['quarter', 'origin'])['share']
        .mean()
        .reset_index()
        .pivot(index='origin', columns='quarter', values='share')
    )
    season_mean.columns = ['Q1 (Jan-Mar)', 'Q2 (Apr-Jun)', 'Q3 (Jul-Sep)', 'Q4 (Oct-Dec)']

    fig, ax = plt.subplots(figsize=(12, 6))
    season_mean.plot(kind='bar', ax=ax, width=0.75)
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{x:.1%}'))
    ax.set_xlabel('Origin Country')
    ax.set_ylabel('Mean Share of Quarterly Arrivals')
    ax.set_title('Seasonal Share by Origin Country (pre-COVID quarterly average)')
    ax.legend(title='Quarter', fontsize=8)
    ax.tick_params(axis='x', rotation=45)
    ax.grid(axis='y', linestyle='--', alpha=0.4)
    plt.tight_layout()
    _save(fig, 'q1_share_seasonality.png')

    print("Q1 EDA completed. Outputs saved to:", OUTPUT_DIR)


if __name__ == "__main__":
    run_eda()
