"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.1
"""
import pandas as pd 
import numpy as np 
import re
import math

from scipy import stats
from scipy.stats import norm

def calculate_payments(df):
    # Convert DataFrame columns to NumPy arrays for efficiency
    interest_rate = df['interest_rate'].to_numpy() / 12
    exposure = df['exposure'].to_numpy()
    loan_term = df['loan_term'].to_numpy()
    unique_id = df['unique_id'].to_numpy()
    date_stamp = df['date_stamp'].to_numpy()
    portfolio = df['portfolio'].to_numpy()
    risk_grade = df['risk_grade'].to_numpy()
    mob = df['mob'].to_numpy()
    param1 = df['param1'].to_numpy()
    param2 = df['param2'].to_numpy()

    # Calculate monthly payment
    monthly_payment = exposure * (interest_rate / (1 - (1 + interest_rate) ** -loan_term))

    # Initialize the opening balance as the loan amount
    opening_balance = exposure.copy()

    # Initialize the t and t+1 counter
    t = 0
    t_plus_1 = t + 1

    # Initialize a list to store the amortization schedules
    amortization_schedules = []

    # While there is still a balance remaining, calculate the interest and principal for the current payment
    while opening_balance.min() > 0:
        # Calculate the interest for the current month
        interest_payment = opening_balance * interest_rate

        # Calculate the principal for the current month
        principal_payment = monthly_payment - interest_payment

        # If the principal payment for this month is greater than the opening balance, \
        #adjust the principal payment and the monthly payment
        mask = opening_balance - principal_payment < 0
        principal_payment[mask] = opening_balance[mask]
        monthly_payment[mask] = principal_payment[mask] + interest_payment[mask]

        # Calculate the closing balance for the current month
        closing_balance = opening_balance - monthly_payment

        # Calculate the average balance for the current month
        average_balance = (opening_balance + closing_balance) / 2

        # Append the calculated values to the amortization schedules
        amortization_schedules.append(
            pd.DataFrame({
                't': t,
                't+1': t_plus_1,
                'opening_balance': opening_balance,
                'monthly_payment': monthly_payment,
                'interest_payment': interest_payment,
                'principal_payment': principal_payment,
                'closing_balance': closing_balance,
                'average_balance': average_balance,
                'unique_id': unique_id,
                'date_stamp': date_stamp,
                'interest_rate': interest_rate,
                'loan_term': loan_term,
                'portfolio': portfolio,
                'risk_grade': risk_grade,
                'mob': mob,
                'param1': param1,
                'param2': param2
            })
        )

        # Update the opening balance for the next month
        opening_balance = closing_balance.copy()

        # Increment the t and t+1 counters
        t += 1
        t_plus_1 += 1

    # Concatenate all the amortization schedules into a single DataFrame
    amortization_schedules = pd.concat(amortization_schedules, ignore_index=True)

    # Sort the DataFrame by 'unique_id' and 't'
    amortization_schedules.sort_values(by=['unique_id', 't'], inplace=True)

    return amortization_schedules

def term_structure(main_df,term_structure_df):
    # Identify the join keys for the term structure data frame
    ts_join_keys = term_structure_df.columns.tolist()

    # Identify the join keys for the main data frame
    main_join_keys = main_df.columns.tolist()
    print(main_join_keys)

    # Identify the join_keys that are common to both data frames
    join_keys = [x for x in ts_join_keys if x in main_df.columns.tolist()]

    # Join the two data frames
    df = pd.merge(main_df, term_structure_df, how='left', on=join_keys)

    return df

# Converts the input string function to an object in the stats library
def rand_func(function):
    # Get the function from the scipy.stats module
    func = getattr(stats, function)
    return func

# Applies the function to every row in the data frame
def any_func_apply(func, df, col_name, *arguments):
    # Apply func to each row of the data frame
    for arg in arguments:
        df[col_name] = func(df[arg]).pmf(df[arg]) * df['remaining_balance']
    return df

def replace_char_and_eval(df, stat_func, specified_column, *args):
    # Find all alphabetical characters in stat_func
    variables = re.findall('[a-zA-Z]', stat_func)

    # Replace the alphabetical characters with the values in each row \ 
    # in the columns with names stored in *args
    for var, value in zip(variables, args):
        stat_func = stat_func.replace(var, 'df[\'' + value + '\']')

    # Evaluate the modified stat_func
    value = eval(stat_func) 
    df[specified_column] = value
    return df

# Main function for calculating the parametric model
def parametric_function(df, stat_func, new_col, specified_column, *args):
    # If stat_func contains a mathematical character, evaluate it
    if any(char in stat_func for char in "=+*/-"):
        # Apply the function to each row of the DataFrame
        df = replace_char_and_eval(df, stat_func, new_col, specified_column, *args)
        return df
    
    # If stat_func contains a function, apply it
    else:
        # Get the distribution function
        p = rand_func(stat_func)

        # Apply function
        df = any_func_apply(p, df, new_col, *args)

        return df
    
def add_param_as_column(risk, main_df, column_name):
    main_df[column_name] = risk
    return main_df

def rename_acc_lvl_columns(df):
    df = df.rename(columns={'loan_id': 'unique_id',
                            'loan_amount': 'exposure',
                            'account_open_date': 'date_stamp'})
    return df

def rename_pd_columns(df):
    df = df.rename(columns={'mob': 't+1'})
    return df

def ECL(df):
    df['EAD'] = df['average_balance']
    df['asset_value_sale'] = df['asset_value']
    df['LGD'] = (df['EAD'] - df['asset_value']) / df['EAD'] 
    df['ECL'] = df['pd']*df['LGD']*df['EAD']
    return df

def total_capital(df):
    df['LTV'] = df['average_balance']/df['asset_value']
    df['TCC_PD'] = df['pd']
    df['EAD_avg'] = df['average_balance']
    df['DT_asset_value_sale'] = df['DT_asset_value']
    df['DT_LGD'] = df['EAD_avg'] - df['DT_asset_value_sale']
    df['def_bal'] = df['average_balance'] * df['pd']
    df['non_def_bal'] = df['average_balance'] - df['def_bal']
    df['corr_coeff'] = 0.03*(1-np.exp(-35*df['TCC_PD']))/(1-np.exp(-35))+0.16*(1-(1-np.exp(-35*df['TCC_PD']))/(1-np.exp(-35)))
    df['non_def_RWA'] = df['DT_LGD'] * (norm.cdf(((1 - df['corr_coeff']) ** -0.5) \
                                * norm.pdf(df['TCC_PD']) + ((df['corr_coeff']\
                                / (1 - df['corr_coeff']))** 0.5 * norm.pdf(0.999)))\
                                - df['TCC_PD'])*df['non_def_bal']*12.5*1.06
    df['def_RWA'] = (12.5*(df['DT_LGD']-df['ECL']/df['average_balance'])*df['def_bal']).clip(lower=0)
    df['total_RWA'] = df['non_def_RWA'] + df['def_RWA']
    df['total_cap'] = df['total_RWA'] * df['capital_ratio']
    return df



