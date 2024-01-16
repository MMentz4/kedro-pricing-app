"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.1
"""
import pandas as pd 
import numpy as np 
import re

from scipy import stats

def calculate_payments(df):
    # Initialize a list to store the amortization schedules
    amortization_schedules = []

    # Apply the calculations to each row in the DataFrame
    for index, row in df.iterrows():
        # Initialize a dictionary to store the calculated values
        calculations = {
            't': [],
            't+1': [],
            'interest_payment': [],
            'principal_payment': [],
            'monthly_payment': [],
            'remaining_balance': [],
            'date_stamp': [],
            'interest_rate': [],
            'loan_term': [],
            'portfolio': [],
            'risk_grade': [],
            'mob': [],
            'param1': [],
            'param2': []
        }

        # Convert annual rate to monthly
        monthly_rate = row.interest_rate / 12

        # Calculate monthly payment
        monthly_payment = row.exposure * (monthly_rate / \
                                          (1 - (1 + monthly_rate) ** -row.loan_term))

        # Initialize the remaining balance as the loan amount
        remaining_balance = row.exposure

        # Initialize the t and t+1 counter
        t = 0
        t_plus_1 = t + 1

        # While there is still a balance remaining, calculate the interest and 
        #principal for the current payment
        while remaining_balance > 0:
            # Calculate the interest for the current month
            interest_payment = remaining_balance * monthly_rate

            # Calculate the principal for the current month
            principal_payment = monthly_payment - interest_payment

            # If the principal payment for this month is greater than \
            # the remaining balance, adjust the principal payment and \
            # the monthly payment
            if remaining_balance - principal_payment < 0:
                principal_payment = remaining_balance
                monthly_payment = principal_payment + interest_payment

            # Append the calculated values to the dictionary
            for key in calculations.keys():
                if key in ['t', 't+1']:
                    calculations[key].append(eval(key))
                elif key in ['interest_payment', 'principal_payment', \
                             'monthly_payment', 'remaining_balance']:
                    calculations[key].append(eval(key.lower().replace(' ', '_')))
                else:
                    calculations[key].append(getattr(row, \
                                                     key.lower().replace(' ', '_')))

            # Update the remaining balance
            remaining_balance -= principal_payment

            # Increment the t and t+1 counters
            t += 1
            t_plus_1 += 1

        # Create a new DataFrame with the calculated values
        amortization_schedule = pd.DataFrame(calculations)

        # Add the unique_id as a new column to the DataFrame
        amortization_schedule['unique_id'] = row.unique_id

        # Add the amortization schedule to the list
        amortization_schedules.append(amortization_schedule)

    # Concatenate all the amortization schedules into a single DataFrame
    amortization_schedules = pd.concat(amortization_schedules, ignore_index=True)

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

def rand_func(function):
    # Get the function from the scipy.stats module
    func = getattr(stats, function)
    return func

def any_func_apply(func, df, col_name, *arguments):
    # Apply func to each row of the data frame
    df[col_name] = df.apply(lambda row: \
                            func(*[row[arg] for arg in arguments]).pmf(*[row[arg] for arg in arguments])\
                                *row['remaining_balance'], axis=1)
    return df

def replace_char_and_eval(row, stat_func, specified_column, *args):
    # Find all alphabetical characters in stat_func
    variables = re.findall('[a-zA-Z]', stat_func)

    # Replace the alphabetical characters with the values in each row \ 
    # in the columns with names stored in *args
    for var, value in zip(variables, args):
        values = row[value]
        stat_func = stat_func.replace(var, str(values))

    return_var = eval(stat_func)
    # Evaluate the modified stat_func
    # return eval(stat_func)*row[specified_column]
    return return_var

def parametric_function(df, stat_func, new_col, specified_column, *args):
    # If stat_func contains a mathematical character, evaluate it
    if any(char in stat_func for char in "=+*/-"):
        # Apply the function to each row of the DataFrame
        df[new_col] = df.apply(replace_char_and_eval, args=(stat_func, specified_column, *args), axis=1)

        return df
    
    # If stat_func contains a function, apply it
    else:
        # Get the distribution function
        p = rand_func(stat_func)

        # Apply function
        df = any_func_apply(p, df, new_col, *args)

        return df
    
def add_param_as_column(risk, main_df, calc_variable, column_name):
    main_df[column_name] = risk * main_df[calc_variable]
    return main_df

def rename_acc_lvl_columns(df):
    df = df.rename(columns={'loan_id': 'unique_id',
                            'loan_amount': 'exposure',
                            'account_open_date': 'date_stamp'})
    return df

def rename_pd_columns(df):
    df = df.rename(columns={'mob': 't+1'})
    return df