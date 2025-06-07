# Build a Dashboard to Visualize Walmart Data 📓



## Import libraries

In this notebook, we're using `pandas` for data handling/wrangling, `numpy` for numerical processing, `datetime` for handling date/time data type and `streamlit` for displaying visual elements (charts and DataFrames).


```
import pandas as pd
import numpy as np
from datetime import datetime
import streamlit as st
import matplotlib.pyplot as plt
from snowflake.snowpark import Session
import calendar
from matplotlib.backends.backend_pdf import PdfPages
from io import BytesIO
```

## Define Functions


```
def render_table(dataframe, title, col_width=3.0, row_height=0.625, font_size=14):
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots(figsize=(col_width * len(dataframe.columns), row_height * len(dataframe)))
    ax.axis('off')
    table = ax.table(cellText=dataframe.values, colLabels=dataframe.columns, loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(font_size)
    table.scale(1, 1.5)
    plt.title(title)
    return fig

```


```
import matplotlib.pyplot as plt

def render_df_to_pdf_table(df, title="", col_width=3.0, row_height=0.625, font_size=12):
    n_rows, n_cols = df.shape

    # Create a figure and axis
    fig, ax = plt.subplots(figsize=(col_width * n_cols, row_height * (n_rows + 1)))
    ax.axis('off')

    # Format values if needed
    formatted_data = df.copy()
    if 'WEEKLY_SALES' in formatted_data.columns:
        formatted_data['WEEKLY_SALES'] = formatted_data['WEEKLY_SALES'].map('{:,.2f}'.format)

    # Create table
    table = ax.table(
        cellText=formatted_data.values,
        colLabels=formatted_data.columns,
        cellLoc='center',
        colLoc='center',
        loc='center'
    )

    # Style
    table.auto_set_font_size(False)
    table.set_fontsize(font_size)
    table.scale(1.1, 1.3)

    # Optional title
    if title:
        plt.title(title, fontsize=font_size + 2, pad=10)

    # Bold header row
    for (row, col), cell in table.get_celld().items():
        if row == 0:
            cell.set_text_props(weight='bold')
            cell.set_facecolor("#f1f3f6")  # Light header color
        elif row % 2 == 0:
            cell.set_facecolor("#fafafa")  # Light row banding
        else:
            cell.set_facecolor("#ffffff")  # Alternating row color

        # Optional: remove table borders for a cleaner look
        cell.set_edgecolor('#dddddd')

    return fig

```

## Weekly Sales by Store and Holiday



```
session = Session.builder.getOrCreate()

# Create DataFrame
# df = pd.DataFrame(data)
df = session.table("GOLD.weekly_sales_by_store").to_pandas()


# df.groupby("ISHOLIDAY")["WEEKLY_SALES"].sum().plot.pie(autopct="%1.1f%%", ylabel="")
# plt.title("Weekly Sales by IsHoliday")
# plt.show()

# Initialize PdfPages
pdf_path = "weekly_sales_report.pdf"
pdf = PdfPages(pdf_path)

# -- STREAMLIT DASHBOARD UI --
st.title("Weekly Sales by Store and Holiday")

# Pie chart: Weekly_Sales by IsHoliday
st.subheader("Weekly_Sales by IsHoliday")
sales_by_isholiday = df.groupby('ISHOLIDAY')['WEEKLY_SALES'].sum()
fig1, ax1 = plt.subplots()
labels = sales_by_isholiday.index.astype(str).tolist()
ax1.pie(sales_by_isholiday, labels=labels, autopct='%1.1f%%', startangle=90)
ax1.axis('equal')
st.pyplot(fig1)


# KPI: Total Weekly Sales
total_sales = df['WEEKLY_SALES'].sum()
st.metric(label="Weekly_Sales", value=f"{total_sales/1e9:.2f}bn")

# KPI: First IsHoliday value in dataset
first_isholiday = str(df['ISHOLIDAY'].iloc[0]).upper()
st.metric(label="First IsHoliday", value=first_isholiday)

# Bar chart: Weekly Sales by Store and IsHoliday
st.subheader("Weekly Sales by Store and IsHoliday")
fig2, ax2 = plt.subplots(figsize=(14, 6))
pivoted = df.pivot_table(
    index='STORE_ID',
    columns='ISHOLIDAY',
    values='WEEKLY_SALES',
    aggfunc='sum',
    fill_value=0
)

pivoted.plot(kind='bar', stacked=False, ax=ax2, color={False: 'dodgerblue', True: 'dimgray'})
ax2.set_ylabel("Weekly Sales")
ax2.set_xlabel("Store")
ax2.set_title("Sales by Store Grouped by IsHoliday")
ax2.legend(["FALSE", "TRUE"])
st.pyplot(fig2)
```

## Weekly Sales by Tempature and Year


```
session = Session.builder.getOrCreate()

# Create DataFrame
# df = pd.DataFrame(data)
df = session.table("GOLD.WEEKLY_SALES_BY_TEMPATURE_YEAR").to_pandas()


# -- STREAMLIT DASHBOARD UI --
st.title("Weekly Sales by Tempature and Year")

# KPI: Total Weekly Sales
total_sales = df['WEEKLY_SALES'].sum()
st.metric(label="Weekly_Sales", value=f"{total_sales/1e9:.2f}bn")

# Bar chart: Weekly Sales by Store and IsHoliday
def bucket_temp(temp):
    if temp < 40: return 'Cold'
    elif temp < 70: return 'Mild'
    else: return 'Hot'

df['TEMP_BUCKET'] = df['STORE_TEMPERATURE'].apply(bucket_temp)

st.subheader("Weekly Sales by Temperature and Year")
fig2, ax2 = plt.subplots(figsize=(14, 6))
pivoted = df.pivot_table(
    index='YEAR',
    columns='TEMP_BUCKET',
    values='WEEKLY_SALES',
    aggfunc='sum',
    fill_value=0
)

pivoted.plot(kind='bar', stacked=False, ax=ax2)
ax2.set_ylabel("Weekly Sales")
ax2.set_xlabel("Year")
ax2.set_title("Sales by Temperature Grouped by Year")
st.pyplot(fig2)
```

## Weekly Sales by Store Size


```
session = Session.builder.getOrCreate()

# Create DataFrame
# df = pd.DataFrame(data)
df = session.table("GOLD.weekly_sales_by_store_size").to_pandas()


# Sort by SIZE just in case
df = df.sort_values(by='SIZE')

# Line chart with matplotlib
st.title("Weekly Sales by Store Size")

fig, ax = plt.subplots(figsize=(10, 5))
ax.plot(df['SIZE'], df['WEEKLY_SALES'], marker='o', linestyle='-')
ax.set_xlabel("Store Size")
ax.set_ylabel("Weekly Sales")
ax.set_title("Line Chart: Weekly Sales vs. Store Size")
st.pyplot(fig)
```

## Weekly Sales by Type and Month


```
session = Session.builder.getOrCreate()

# Create DataFrame
# df = pd.DataFrame(data)
df = session.table("GOLD.weekly_sales_by_store_type_month").to_pandas()

# Ensure MONTH is int (1-12)
df['MONTH'] = df['MONTH'].astype(int)

# Generate month name from number
df['MONTH_NAME'] = df['MONTH'].apply(lambda x: calendar.month_name[x])

# Define proper chronological order
month_order = [calendar.month_name[i] for i in range(1, 13)]

# Set MONTH_NAME as ordered categorical
df['MONTH_NAME'] = pd.Categorical(df['MONTH_NAME'], categories=month_order, ordered=True)

# Sort BEFORE pivoting
df = df.sort_values('MONTH_NAME')

pivoted = df.pivot_table(
    index='MONTH_NAME',
    columns='TYPE',
    values='WEEKLY_SALES',
    aggfunc='sum',
    fill_value=0,
    observed=False
)

# Plot
st.title("Weekly Sales by Type and Month")

fig, ax = plt.subplots(figsize=(12, 6))
pivoted.plot(ax=ax, marker='o', linewidth=2)
ax.set_ylabel("Weekly Sales")
ax.set_xlabel("Month")
ax.set_title("Weekly Sales Trends by Store Type")
ax.legend(title='Type')
plt.xticks(rotation=45)
st.pyplot(fig)
```

## Markdown Sales by Store and Year


```
# Start Snowflake session
session = Session.builder.getOrCreate()

# Load the markdown sales data
df = session.table("WALMART.GOLD.MARKDOWN_SALES_YEAR_STORE").to_pandas()

# Group and aggregate markdown totals by year
agg_df = df.groupby("YEAR")[['MARKDOWN1', 'MARKDOWN2', 'MARKDOWN3', 'MARKDOWN4', 'MARKDOWN5']].sum().reset_index()

# Chart layout
st.title("Markdown Sales by Store and Year")
st.subheader("Group Bar Chart of Markdown Components")

# Plotting
fig, ax = plt.subplots(figsize=(10, 6))
bar_width = 0.15
years = agg_df['YEAR'].astype(str)
x = range(len(years))
markdown_cols = ['MARKDOWN1', 'MARKDOWN2', 'MARKDOWN3', 'MARKDOWN4', 'MARKDOWN5']
colors = ['cornflowerblue', 'gray', 'tomato', 'sandybrown', 'dimgray']

for i, col in enumerate(markdown_cols):
    bar = ax.bar(
        [xi + i * bar_width for xi in x],
        agg_df[col] / 1e9,  # convert to billions for better scale
        width=bar_width,
        label=col,
        color=colors[i]
    )
    # Add value labels
    for rect in bar:
        height = rect.get_height()
        ax.annotate(f'{height:.2f}bn',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=8)

# Styling
ax.set_xlabel("Year")
ax.set_ylabel("Markdowns (in Billions)")
ax.set_title("Markdown1, Markdown2, Markdown3, Markdown4 and Markdown5 by Year")
ax.set_xticks([r + bar_width * 2 for r in x])
ax.set_xticklabels(years)
ax.legend(title="Type")
st.pyplot(fig)


# PIVOT TABLE DISPLAY ---
st.subheader("Markdown Sales by Year and Store")
pivot_table = df.groupby(['YEAR', 'STORE_ID'])[
    ['MARKDOWN1', 'MARKDOWN2', 'MARKDOWN3', 'MARKDOWN4', 'MARKDOWN5']
].sum().reset_index()

# format large numbers
styled_pivot = pivot_table.style.format({
    'MARKDOWN1': '{:,.2f}',
    'MARKDOWN2': '{:,.2f}',
    'MARKDOWN3': '{:,.2f}',
    'MARKDOWN4': '{:,.2f}',
    'MARKDOWN5': '{:,.2f}',
})
fig_table = render_table(pivot_table, title="Markdown Sales by Year and Store")
st.dataframe(styled_pivot, use_container_width=True)
```

## Weekly Sales by Store and Type


```
# Start Snowflake session
session = Session.builder.getOrCreate()

# Load the assumed data: includes TYPE, STORE_ID, WEEKLY_SALES
df = session.table("GOLD.WEEKLY_SALES_BY_STORE_TYPE").to_pandas()

# Format and sort
df['WEEKLY_SALES_M'] = df['WEEKLY_SALES'] / 1e6  # Convert to millions
df = df.sort_values(['TYPE', 'WEEKLY_SALES_M'])

# Chart
st.title("Weekly Sales by Store and Type")
st.subheader("Horizontal Grouped Bar Chart")

# Create figure
fig, ax = plt.subplots(figsize=(12, 8))

# Plot bars grouped by TYPE
colors = plt.cm.get_cmap("tab20", df['STORE_ID'].nunique())
for i, store_type in enumerate(df['TYPE'].unique()):
    type_df = df[df['TYPE'] == store_type]
    ax.barh(
        y=[f"{store_type}-{int(s)}" for s in type_df['STORE_ID']],
        width=type_df['WEEKLY_SALES_M'],
        label=store_type
    )

# Formatting
ax.set_xlabel("Weekly Sales (in Millions)")
ax.set_ylabel("Store-Type")
ax.set_title("Weekly Sales by Type and Store")
ax.legend(title="Store Type")
plt.tight_layout()
st.pyplot(fig)



```

## Fuel Price by Store and Year


```
# Start session and load the data
session = Session.builder.getOrCreate()
df = session.table("WALMART.GOLD.FUEL_PRICE_BY_STORE_YEAR").to_pandas()

# Pivot table: Average fuel price per store per year
pivot_table = df.pivot_table(
    index='STORE_ID',
    columns='YEAR',
    values='FUEL_PRICE',
    aggfunc='mean',
    fill_value=0
)

# Add row total
pivot_table['Total'] = pivot_table.sum(axis=1)

# Add column total
totals_row = pivot_table.sum(numeric_only=True).to_frame().T
totals_row.index = ['Total']

# Combine pivot with totals row
final_table = pd.concat([pivot_table, totals_row])
final_table.columns = final_table.columns.map(str)


# Format
styled = final_table.style.format("{:,.2f}")

# Display in Streamlit
st.title("Fuel Price by Store and Year")
st.dataframe(styled, use_container_width=True)

fig_table = render_table(final_table, title="Fuel Price by Store and Year")




```

## Weekly Sales Breakdown


```
# Start Snowflake session and load the view
session = Session.builder.getOrCreate()
df = session.table("GOLD.weekly_sales_breakdown").to_pandas()

# Normalize column names to uppercase (if not already)
df.columns = df.columns.str.upper()

# Convert to billions and sort
df['WEEKLY_SALES_B'] = df['WEEKLY_SALES'] / 1e9

# ----- Chart 1: Weekly Sales by Year -----
st.subheader("Weekly Sales by Year")
sales_by_year = df.groupby('SALES_YEAR')['WEEKLY_SALES_B'].sum().reset_index()

fig1, ax1 = plt.subplots()
ax1.bar(sales_by_year['SALES_YEAR'].astype(str), sales_by_year['WEEKLY_SALES_B'], color='deepskyblue')
ax1.set_ylabel("Sales (in Billions)")
for i, val in enumerate(sales_by_year['WEEKLY_SALES_B']):
    ax1.text(i, val + 0.01, f"{val:.2f}bn", ha='center', fontsize=8)
st.pyplot(fig1)

# ----- Chart 2: Weekly Sales by Month -----
st.subheader("Weekly Sales by Month")
month_order = ['January', 'February', 'March', 'April', 'May', 'June',
               'July', 'August', 'September', 'October', 'November', 'December']
df['SALES_MONTH'] = pd.Categorical(df['SALES_MONTH'], categories=month_order, ordered=True)
sales_by_month = (
    df.groupby('SALES_MONTH', observed=False)['WEEKLY_SALES_B']
    .sum()
    .reindex(month_order)
    .reset_index()
)

fig2, ax2 = plt.subplots()
ax2.bar(sales_by_month['SALES_MONTH'], sales_by_month['WEEKLY_SALES_B'], color='deepskyblue')
ax2.set_ylabel("Sales (in Billions)")
plt.xticks(rotation=45)
for i, val in enumerate(sales_by_month['WEEKLY_SALES_B']):
    ax2.text(i, val + 0.01, f"{val:.2f}bn", ha='center', fontsize=8)
st.pyplot(fig2)

# ----- Chart 3: Weekly Sales by Day -----
st.subheader("Weekly Sales by Day")
sales_by_day = df.groupby('SALES_DAY')['WEEKLY_SALES'].sum().reset_index()

fig3, ax3 = plt.subplots()
ax3.bar(sales_by_day['SALES_DAY'], sales_by_day['WEEKLY_SALES'] / 1e6, color='deepskyblue')
ax3.set_ylabel("Sales (in Millions)")
ax3.set_xlabel("Day of Month")
for i, val in enumerate(sales_by_day['WEEKLY_SALES']):
    ax3.text(sales_by_day['SALES_DAY'][i], val / 1e6 + 5, f"{val/1e6:.0f}M", ha='center', fontsize=8)
st.pyplot(fig3)



```

## Weekly Sales by CPI


```
# Start session and load the dbt view
session = Session.builder.getOrCreate()
df = session.table("GOLD.WEEKLY_SALES_BY_CPI").to_pandas()

# Normalize column names to uppercase
df.columns = df.columns.str.upper()

# Sort by CPI (already numeric now)
df = df.sort_values(by='CPI')

# Plot
st.title("Weekly Sales by CPI")
st.subheader("Weekly_Sales by CPI")

fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(df['CPI'], df['WEEKLY_SALES'] / 1e6, linestyle='-', marker='.', color='deepskyblue')
ax.set_xlabel("CPI")
ax.set_ylabel("Weekly Sales (in Millions)")
ax.set_title("Weekly Sales by CPI")

# Annotate top 10 points
top_points = df.sort_values('WEEKLY_SALES', ascending=False).head(10)
for _, row in top_points.iterrows():
    ax.annotate(f"{row['WEEKLY_SALES'] / 1e6:.1f}M",
                (row['CPI'], row['WEEKLY_SALES'] / 1e6),
                textcoords="offset points", xytext=(0, 6), ha='center', fontsize=8)

st.pyplot(fig)

```

## Department Wise Weekly Sales


```
# Start session and load the view
session = Session.builder.getOrCreate()
df = session.table("GOLD.WEEKLY_SALES_BY_DEPT").to_pandas()
df.columns = df.columns.str.upper()

# Convert to billions for total metric
total_sales = df['WEEKLY_SALES'].sum()
top5 = df.nlargest(5, 'WEEKLY_SALES')

# Display dashboard
st.title("Department Wise Weekly Sales")

# KPI
st.metric(label="Weekly_Sales", value=f"{total_sales / 1e9:.3f}bn")

# Top 5 departments
st.subheader("Top 5 Department-Wise Sales")
st.dataframe(top5.style.format({"WEEKLY_SALES": "{:,.2f}"}), use_container_width=True)

fig_top5 = render_df_to_pdf_table(top5, title="Top 5 Department-Wise Sales")

# Full department table
st.subheader("All Department Sales")
st.dataframe(df.sort_values("DEPT_ID").style.format({"WEEKLY_SALES": "{:,.2f}"}), use_container_width=True)

fig_all = render_df_to_pdf_table(df, title="All Department Sales")

# Bar chart
st.subheader("Weekly_Sales by Department")
fig, ax = plt.subplots(figsize=(14, 6))
ax.bar(df['DEPT_ID'].astype(str), df['WEEKLY_SALES'] / 1e9, color='slateblue')
ax.set_xlabel("Department")
ax.set_ylabel("Sales (in Billions)")
ax.set_title("Weekly Sales by Department")

# Annotate top spikes
top_labels = df.nlargest(10, 'WEEKLY_SALES')
for _, row in top_labels.iterrows():
    ax.annotate(f"{row['WEEKLY_SALES']/1e9:.2f}bn", 
                (str(row['DEPT_ID']), row['WEEKLY_SALES']/1e9),
                textcoords="offset points", xytext=(0,5), ha='center', fontsize=8)

st.pyplot(fig)


# Create a BytesIO buffer to hold the PDF data
pdf_buffer = BytesIO()

# Initialize PdfPages with the buffer
with PdfPages(pdf_buffer) as pdf:
    # Retrieve all figure numbers
    fig_nums = plt.get_fignums()
    # Iterate through each figure and save it to the PDF
    for fig_num in fig_nums:
        fig = plt.figure(fig_num)
        pdf.savefig(fig)
        plt.close(fig)  # Close the figure after saving to free up memory

# Reset buffer position to the beginning
pdf_buffer.seek(0)

st.download_button(
    label="📄 Download All Charts as PDF",
    data=pdf_buffer,
    file_name="all_charts.pdf",
    mime="application/pdf"
)

```
