"""
This module is for your final visualization code.
After you have done your EDA and wish to create some visualizations for you final jupyter notebook
A framework for each type of visualization is provided.
"""
# visualization packages
import matplotlib.pyplot as plt
from matplotlib.axes._axes import _log as matplotlib_axes_logger
from matplotlib.ticker import FuncFormatter
import seaborn as sns

# Standard data manipulation packages
import pandas as pd
import numpy as np

# useful builtin python packages
import functools

matplotlib_axes_logger.setLevel('ERROR')

# Set specific parameters for the visualizations
large = 22; med = 16; small = 12
params = {'axes.titlesize': large,
          'legend.fontsize': med,
          'figure.figsize': (16, 10),
          'axes.labelsize': med,
          'xtick.labelsize': med,
          'ytick.labelsize': med,
          'figure.titlesize': large}
plt.rcParams.update(params)
plt.style.use('seaborn-whitegrid')
sns.set_style("white")


def number_movie_pie(cleaned_data, save_filename=None):
    '''
    Pie chart.
    '''
    number_of_movie = cleaned_data.groupby("profit_status").size()
    pie, ax = plt.subplots(figsize=[10,6])
    labels = number_of_movie.keys()
    plt.pie(x=number_of_movie, autopct="%.2f%%", labels=labels, pctdistance=0.5, textprops={'fontsize': 14})
    plt.title("Percent of Movie Which Are Profitable", fontsize=16, weight = 'bold');
    
    
def worldwide_gross_pie(cleaned_data):
    '''
    Pie chart.
    '''
    is_profit = cleaned_data.groupby("profit_status")["worldwide_gross"].sum()
    pie, ax = plt.subplots(figsize=[10,6])
    labels = is_profit.keys()
    plt.pie(x=is_profit, autopct="%.2f%%", labels=labels, pctdistance=0.5, textprops={'fontsize': 14})
    plt.title("Percent of Profit/Non-Profit Movie Worldwide Gross", fontsize=16, weight = 'bold');
    
    
def production_budget_pie(cleaned_data):    
    '''
    Pie chart.
    '''
    is_profit = cleaned_data.groupby("profit_status")["production_budget"].sum()
    pie, ax = plt.subplots(figsize=[10,6])
    labels = is_profit.keys()
    plt.pie(x=is_profit, autopct="%.2f%%", labels=labels, pctdistance=0.5, textprops={'fontsize': 14})
    plt.title("Percent of Profit/Non-Profit Movie Production Budget", fontsize=16, weight = 'bold');

    
def movies_profit(cleaned_data, save_filename=None):
    '''
    Stack Bar chart.
    '''
    budget_no_profit_movies = cleaned_data[cleaned_data['ROI'] <= 0]['production_budget'].sum()
    budget_profit_movies = cleaned_data[cleaned_data['ROI'] > 0]['production_budget'].sum()
    worldwide_no_profit_movies = cleaned_data[cleaned_data['ROI'] <= 0]['worldwide_gross'].sum()
    worldwide_profit_movies = cleaned_data[cleaned_data['ROI'] > 0]['worldwide_gross'].sum()
    foo= pd.DataFrame([('production_budget', 'no_profit_movies',budget_no_profit_movies),
                       ('production_budget', 'profit_movies', budget_profit_movies),
                       ('production_budget', 'all_movies', budget_profit_movies + budget_no_profit_movies),
                       ('worldwide_gross', 'no_profit_movies', worldwide_no_profit_movies),
                       ('worldwide_gross', 'profit_movies', worldwide_profit_movies),
                       ('worldwide_gross', 'all_movies', worldwide_no_profit_movies + worldwide_profit_movies)],
                      columns=('type_money', 'movie_category', 'amount'))
    foo2= pd.DataFrame([(budget_no_profit_movies, budget_profit_movies),
                       (worldwide_no_profit_movies, worldwide_profit_movies)],
                       index=["production_budget", "worldwide_gross"],
                      columns=('no_profit_movies', 'profit_movies'))
    ax = foo2.plot.bar(stacked=True)
    plt.xticks(rotation=90)

    if save_filename:
        ax.savefig(save_filename)
    
    
def vote(cleaned_data, save_filename=None):
    '''
    Scatter plot with regression.
    '''
    ax = sns.regplot(data=cleaned_data[cleaned_data["ROI"] < 100],
            x="vote_average", 
            y="ROI",
            x_jitter=True,
            line_kws = {'color':'black'})
    if save_filename:
        ax.savefig(save_filename)
    
    
def runtime(cleaned_data, save_filename=None):
    '''
    Scatter plot with regression.
    '''
    ax = sns.regplot(data=cleaned_data[cleaned_data["ROI"] < 100],
            x="runtime_minutes", 
            y="ROI",
            x_jitter=True,
            line_kws = {'color':'black'})
    if save_filename:
        ax.savefig(save_filename)
        
    
def cost(cleaned_data, save_filename=None):
    '''
    Scatter plot with regression.
    '''
    sns.regplot(data=cleaned_data[cleaned_data["ROI"] < 100],
            x="runtime_minutes", 
            y="production_budget",
            x_jitter=True,
            color="blue")
    ax = sns.regplot(data=cleaned_data[cleaned_data["ROI"] < 100],
            x="runtime_minutes", 
            y="profit",
            x_jitter=True,
            color="Red")
    if save_filename:
        ax.savefig(save_filename)
        
    
def budget_roi(cleaned_data, save_filename=None):
    '''
    Scatter plot with regression.
    '''
    ax = sns.regplot(data=cleaned_data[cleaned_data["ROI"] < 100],
            x="production_budget", 
            y="ROI",
            x_jitter=True,
            line_kws = {'color':'black'})
    if save_filename:
        ax.savefig(save_filename)
        
        
def roi_production_budget_scatter_20(cleaned_data, save_filename=None):
    '''
    Scatter plot with regression, color base on profit status.
    '''
    sns.regplot(data=cleaned_data[(cleaned_data["production_budget"] > cleaned_data['production_budget'].quantile(.2)) & 
                                  (cleaned_data["production_budget"] < cleaned_data['production_budget'].quantile(.6))],
                scatter=False,
                x="production_budget", 
                y="ROI",
                x_jitter=True,
                line_kws = {'color':'black'})
    # Plot points that make money Blue
    sns.regplot(data=cleaned_data[(cleaned_data["production_budget"] > cleaned_data['production_budget'].quantile(.2)) & 
                                  (cleaned_data["production_budget"] < cleaned_data['production_budget'].quantile(.6)) & 
                                  (cleaned_data["profit"] > 0)],
                fit_reg=False,
                x="production_budget", 
                y="ROI",
                x_jitter=True,
                color="blue")
    # Plot points that lose money Red
    ax = sns.regplot(data=cleaned_data[(cleaned_data["production_budget"] > cleaned_data['production_budget'].quantile(.2)) & 
                                  (cleaned_data["production_budget"] < cleaned_data['production_budget'].quantile(.6)) & 
                                  (cleaned_data["profit"] <= 0)],
                fit_reg=False,
                x="production_budget", 
                y="ROI",
                x_jitter=True,
                color="red")
    if save_filename:
        ax.savefig(save_filename)
        
    
def roi_production_budget_scatter_60(cleaned_data, save_filename=None):
    '''
    Scatter plot with regression, color base on profit status.
    '''
    sns.regplot(data=cleaned_data[cleaned_data["production_budget"] > cleaned_data['production_budget'].quantile(.6)],
                scatter=False,
                x="production_budget", 
                y="ROI",
                x_jitter=True)
    # Plot points that make money Blue
    sns.regplot(data=cleaned_data[(cleaned_data["production_budget"] > cleaned_data['production_budget'].quantile(.6)) & 
                                  (cleaned_data["profit"] > 0)],
                fit_reg=False,
                x="production_budget", 
                y="ROI",
                x_jitter=True,
                color="blue")
    # Plot points that lose money Red
    ax = sns.regplot(data=cleaned_data[(cleaned_data["production_budget"] > cleaned_data['production_budget'].quantile(.6)) & 
                                  (cleaned_data["profit"] <= 0)],
                fit_reg=False,
                x="production_budget", 
                y="ROI",
                x_jitter=True,
                color="red")
    if save_filename:
        ax.savefig(save_filename)
        

def column_quantile_analysis(cleaned_data, column, num_quantiles=5, save_filename=None):
    '''
    Bar chart showing ROI and profit of each movie category, split by quantile of input column.
    '''
    quantile_size = 100/num_quantiles
    binned_data = cleaned_data.copy()
    binned_data["bin"] = pd.qcut(binned_data[column], 
                                              num_quantiles, 
                                              labels=[f"{quantile_size*bin_num}-{quantile_size * (bin_num+1)}" 
                                                      for bin_num in range(num_quantiles)])
    columns = ["production_budget", "profit","bin"]
    if column not in columns:
        columns.append(column)
    bin_summary = binned_data[columns].groupby("bin").sum()
    bin_summary["ROI"] = bin_summary["profit"] / bin_summary["production_budget"]
                                
    ax = sns.barplot(data=bin_summary.reset_index(),x="bin", y="ROI")
    ax.set_xlabel(f"{column} percentile")
    
    quantile_cutoffs = {
                            f"{quantile_size*bin_num}":
                            cleaned_data[column].quantile(bin_num/num_quantiles) 
                            for bin_num in range(0, num_quantiles + 1)
    }
    if save_filename:
        ax.savefig(save_filename)
    return (bin_summary, quantile_cutoffs, ax)


def column_quantile_analysis2(cleaned_data, column, num_quantiles=5, format_string=".1f", save_filename=None):
    '''
    Bar chart showing ROI and profit of each movie category, split by quantile of input column.
    '''
    quantile_size = 1/num_quantiles
    binned_data = cleaned_data.copy()
    quantile_cutoffs= ['']
    for bin_num in range(1, num_quantiles):
        quantile_cutoffs.append(f"{cleaned_data[column].quantile(bin_num*quantile_size):{format_string}}")
    quantile_cutoffs.append('')
    binned_data["bin"] = pd.qcut(binned_data[column], 
                                              num_quantiles, 
                                              labels=[f"{quantile_cutoffs[bin_num]}\n-\n{quantile_cutoffs[bin_num+1]}" 
                                                      for bin_num in range(num_quantiles)])
    columns = ["production_budget", "profit","bin"]
    if column not in columns:
        columns.append(column)
    bin_summary = binned_data[columns].groupby("bin").sum()
    bin_summary["ROI"] = bin_summary["profit"] / bin_summary["production_budget"]
    roi_fig, roi_ax = plt.subplots()
    sns.barplot(data=bin_summary.reset_index(),x="bin", y="ROI", ax=roi_ax)
    roi_ax.set_xlabel(f"{column}")
    profit_fig, profit_ax = plt.subplots()
    sns.barplot(data=bin_summary.reset_index(),x="bin", y="profit", ax=profit_ax)
    profit_ax.set_xlabel(f"{column}")
    if save_filename:
        roi_ax.savefig(save_filename + "roi")
    return (bin_summary, roi_ax, profit_ax)      


def data_to_plot(cleaned_data, save_filename=None):
    """
    Box plot for studio ROI.
    """
    data_to_plot = cleaned_data.copy()
    foo = data_to_plot['studio'].value_counts()
    data_to_plot["studio_num_movies"] = data_to_plot['studio'].map(foo)
    data_to_plot = data_to_plot[data_to_plot["studio_num_movies"] > 1]
    studios_by_avg_roi_desc = list(data_to_plot.groupby("studio")['ROI'].mean().sort_values(ascending=False).index)
    boxplot = sns.boxplot(x="studio", y="ROI", data=data_to_plot, order=studios_by_avg_roi_desc)
    plt.xticks(rotation=30)

    if save_filename:
        boxplot.savefig(save_filename)
        
    return boxplot

    
def profitability_movies(cleaned_data, save_filename=None):
    '''
    Hist chart.    
    '''
    plt.hist(cleaned_data[cleaned_data['ROI'] < 20]['ROI'], bins=21, edgecolor='black')
    plt.xlabel('Return On Investment')
    plt.ylabel('Number of Movies')
    plt.title('Profitability of Movies')
    plt.show()
#     if save_filename:
#         sns_plot.savefig(save_filename)


def calculate_average_roi(df):
    '''
    Average ROI.
    '''
    return df["profit"].sum() / df["production_budget"].sum()


def calculate_average_roi_for_genre(df, genre):
    """
    Average ROI for genre.
    """
    return calculate_average_roi(df[df["genres"].str.contains(genre)])

        
def get_genre_counts_roi_and_profit(df):
    """
    Explode genre string and return statistics based on the genre.
    """
    explodey_data = df.copy()
    explodey_data["genres"] = explodey_data["genres"].str.split(",")
    sploded_data = explodey_data.explode("genres")
    counts = sploded_data["genres"].value_counts()
    roi_by_genre = pd.Series(counts.index.map(functools.partial(calculate_average_roi_for_genre, df)),
                         index=counts.index)
    profit_by_genre_unordered = sploded_data.groupby("genres").sum()["profit"]
    profit_by_genre = pd.Series(counts.index.map(lambda g: profit_by_genre_unordered[g]),
                         index=counts.index)
    return pd.concat({"count": counts, "ROI": roi_by_genre, "profit": profit_by_genre}, axis=1)


def genre(movies):
    """
    Bar chart for ROI and profit for each Genre. 
    """
    counts_and_roi_by_genre = get_genre_counts_roi_and_profit(movies)
    count_fig, count_ax = plt.subplots()
    sns.barplot(x=counts_and_roi_by_genre.index, y=counts_and_roi_by_genre["count"], ax=count_ax)
    plt.xticks(rotation=30)

    roi_fig, roi_ax = plt.subplots()
    sns.barplot(x=counts_and_roi_by_genre.index, y=counts_and_roi_by_genre["ROI"], ax=roi_ax)
    plt.xticks(rotation=30)
        
    profit_fig, profit_ax = plt.subplots()
    sns.barplot(x=counts_and_roi_by_genre.index, y=counts_and_roi_by_genre["profit"], ax=profit_ax)
    plt.xticks(rotation=30)
    avg_roi = calculate_average_roi(movies)
    profit_ax.axhline(avg_roi, ls='--')
    roi_ax.axhline(avg_roi, ls='--')

    return count_ax, roi_ax, profit_ax, avg_roi,counts_and_roi_by_genre
