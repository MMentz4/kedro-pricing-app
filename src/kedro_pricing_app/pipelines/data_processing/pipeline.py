"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.1
"""

from kedro.pipeline import Pipeline, pipeline, node

# from .nodes import rename_acc_lvl_columns\
#                 ,rename_pd_columns\
#                 ,calculate_payments\
#                 ,term_structure\
#                 ,parametric_function\
#                 ,rand_func\
#                 ,any_func_apply\
#                 ,replace_char_and_eval\
#                 ,add_param_as_column\
#                 ,ECL\
#                 ,total_capital
#                 

from .nodes import *

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=rename_acc_lvl_columns,
            inputs='acc_lvl_data',
            outputs='renamed_acc_lvl_data',
            name='rename_acc_lvl_data_node'
        ),
        node(
            func=rename_pd_columns,
            inputs='pd_data',
            outputs='renamed_pd_data',
            name='rename_pd_data_node'
        ),
        node(
            func=calculate_payments,
            inputs='renamed_acc_lvl_data',
            outputs='amortization_schedules',
            name='calculate_payments_node'
        ),
        node(
            func=term_structure,
            inputs=['amortization_schedules','renamed_pd_data'],
            outputs='term_structure_output_df',
            name='term_structure_node'
        ),
        node(
            func=add_param_as_column,
            inputs=['params:risk', 'term_structure_output_df', 'params:column_name'],
            outputs='single_param_output_df',
            name='add_param_as_column_node'
        ),
        node(
            func=parametric_function,
            inputs=['single_param_output_df', 'params:stat_func', 'params:new_col', 'params:arg1', 'params:arg2'],
            outputs='param_func_output_df',
            name='parametric_function_node'
        ),
         node(
            func=term_structure,
            inputs=['param_func_output_df','asset_value'],
            outputs='asset_value_output_df',
            name='asset_value_node'
        ),
        node(
            func=ECL,
            inputs=['asset_value_output_df'],
            outputs='ecl_output_df',
            name='ECL_node'
        ),
        node(
            func=term_structure,
            inputs=['ecl_output_df','DT_asset_value'],
            outputs='DT_asset_value_output_df',
            name='DT_asset_value_node'
        ),
        node(
            func=add_param_as_column,
            inputs=['params:capital_ratio', 'DT_asset_value_output_df', 'params:col_name'],
            outputs='cap_ratio_output_df',
            name='add_cap_ratio_node'
        ),
        node(
            func=total_capital,
            inputs=['cap_ratio_output_df'],
            outputs='total_capital_output_df',
            name='total_capital_node'
        )
    ])
