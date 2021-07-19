# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import logging
import traceback


config = {
    "DataServiceName": "NGE",
    "configuration": {
        "SLS_IQVIA": {
            "data_month_mapping": {
                "Time_ID": {
                    "DOLLARS_DATA_MONTH_1": "202104",
                    "DOLLARS_DATA_MONTH_2": "202103",
                    "DOLLARS_DATA_MONTH_3": "202102",
                    "DOLLARS_DATA_MONTH_4": "202101",
                    "DOLLARS_DATA_MONTH_5": "202012",
                    "DOLLARS_DATA_MONTH_6": "202011",
                    "DOLLARS_DATA_MONTH_7": "202010",
                    "DOLLARS_DATA_MONTH_8": "202009",
                    "DOLLARS_DATA_MONTH_9": "202008",
                    "DOLLARS_DATA_MONTH_10": "202007",
                    "DOLLARS_DATA_MONTH_11": "202006",
                    "DOLLARS_DATA_MONTH_12": "202005",
                    "DOLLARS_DATA_MONTH_13": "202004",
                    "DOLLARS_DATA_MONTH_14": "202003",
                    "DOLLARS_DATA_MONTH_15": "202002",
                    "DOLLARS_DATA_MONTH_16": "202001",
                    "DOLLARS_DATA_MONTH_17": "201912",
                    "DOLLARS_DATA_MONTH_18": "201911",
                    "DOLLARS_DATA_MONTH_19": "201910",
                    "DOLLARS_DATA_MONTH_20": "201909",
                    "DOLLARS_DATA_MONTH_21": "201908",
                    "DOLLARS_DATA_MONTH_22": "201907",
                    "DOLLARS_DATA_MONTH_23": "201906",
                    "DOLLARS_DATA_MONTH_24": "201905"
                },
                "Dollars_Data": {
                    "id_vars": [
                        "CLIENT_NUMBER",
                        "REPORT_NUMBER",
                        "DDD_SUBCATEGORY_CODE",
                        "SRA3_FACILITY_OUTLET_ASN",
                        "SRA_4",
                        "CATEGORY_CODE",
                        "PRODUCT_GROUP",
                        "DATA_DATE",
                        "OUTLET_NAME",
                        "OUTLET_ADDRESS",
                        "OUTLET_CITY",
                        "OUTLET_STATE",
                        "OUTLET_ZIP",
                        "FILE_INST_ID",
                        "RCD_STAT",
                        "AUDIT_BATCH_ID",
                        "AUDIT_INSRT_DT",
                        "AUDIT_INSRT_NM",
                        "AUDIT_UPDT_NM"
                    ],
                    "value_vars": [
                        "DOLLARS_DATA_MONTH_1",
                        "DOLLARS_DATA_MONTH_2",
                        "DOLLARS_DATA_MONTH_3",
                        "DOLLARS_DATA_MONTH_4",
                        "DOLLARS_DATA_MONTH_5",
                        "DOLLARS_DATA_MONTH_6",
                        "DOLLARS_DATA_MONTH_7",
                        "DOLLARS_DATA_MONTH_8",
                        "DOLLARS_DATA_MONTH_9",
                        "DOLLARS_DATA_MONTH_10",
                        "DOLLARS_DATA_MONTH_11",
                        "DOLLARS_DATA_MONTH_12",
                        "DOLLARS_DATA_MONTH_13",
                        "DOLLARS_DATA_MONTH_14",
                        "DOLLARS_DATA_MONTH_15",
                        "DOLLARS_DATA_MONTH_16",
                        "DOLLARS_DATA_MONTH_17",
                        "DOLLARS_DATA_MONTH_18",
                        "DOLLARS_DATA_MONTH_19",
                        "DOLLARS_DATA_MONTH_20",
                        "DOLLARS_DATA_MONTH_21",
                        "DOLLARS_DATA_MONTH_22",
                        "DOLLARS_DATA_MONTH_23",
                        "DOLLARS_DATA_MONTH_24"
                    ]
                },
                "Units_Data": {
                    "id_vars": [
                        "CLIENT_NUMBER",
                        "REPORT_NUMBER",
                        "DDD_SUBCATEGORY_CODE",
                        "SRA3_FACILITY_OUTLET_ASN",
                        "SRA_4",
                        "CATEGORY_CODE",
                        "PRODUCT_GROUP",
                        "DATA_DATE",
                        "OUTLET_NAME",
                        "OUTLET_ADDRESS",
                        "OUTLET_CITY",
                        "OUTLET_STATE",
                        "OUTLET_ZIP",
                        "FILE_INST_ID",
                        "RCD_STAT",
                        "AUDIT_BATCH_ID",
                        "AUDIT_INSRT_DT",
                        "AUDIT_INSRT_NM",
                        "AUDIT_UPDT_NM"
                    ],
                    "value_vars": [
                        "UNITS_DATA_MONTH_1",
                        "UNITS_DATA_MONTH_2",
                        "UNITS_DATA_MONTH_3",
                        "UNITS_DATA_MONTH_4",
                        "UNITS_DATA_MONTH_5",
                        "UNITS_DATA_MONTH_6",
                        "UNITS_DATA_MONTH_7",
                        "UNITS_DATA_MONTH_8",
                        "UNITS_DATA_MONTH_9",
                        "UNITS_DATA_MONTH_10",
                        "UNITS_DATA_MONTH_11",
                        "UNITS_DATA_MONTH_12",
                        "UNITS_DATA_MONTH_13",
                        "UNITS_DATA_MONTH_14",
                        "UNITS_DATA_MONTH_15",
                        "UNITS_DATA_MONTH_16",
                        "UNITS_DATA_MONTH_17",
                        "UNITS_DATA_MONTH_18",
                        "UNITS_DATA_MONTH_19",
                        "UNITS_DATA_MONTH_20",
                        "UNITS_DATA_MONTH_21",
                        "UNITS_DATA_MONTH_22",
                        "UNITS_DATA_MONTH_23",
                        "UNITS_DATA_MONTH_24"
                    ]
                },
                "requiredFlag": "Y"
            }
        },
        "DQ_Check": {
            "DollarSumCheck": {
                "STGDollarColumn": ["DOLLARS_DATA_MONTH_1",
                                    "DOLLARS_DATA_MONTH_2",
                                    "DOLLARS_DATA_MONTH_3",
                                    "DOLLARS_DATA_MONTH_4",
                                    "DOLLARS_DATA_MONTH_5",
                                    "DOLLARS_DATA_MONTH_6",
                                    "DOLLARS_DATA_MONTH_7",
                                    "DOLLARS_DATA_MONTH_8",
                                    "DOLLARS_DATA_MONTH_9",
                                    "DOLLARS_DATA_MONTH_10",
                                    "DOLLARS_DATA_MONTH_11",
                                    "DOLLARS_DATA_MONTH_12",
                                    "DOLLARS_DATA_MONTH_13",
                                    "DOLLARS_DATA_MONTH_14",
                                    "DOLLARS_DATA_MONTH_15",
                                    "DOLLARS_DATA_MONTH_16",
                                    "DOLLARS_DATA_MONTH_17",
                                    "DOLLARS_DATA_MONTH_18",
                                    "DOLLARS_DATA_MONTH_19",
                                    "DOLLARS_DATA_MONTH_20",
                                    "DOLLARS_DATA_MONTH_21",
                                    "DOLLARS_DATA_MONTH_22",
                                    "DOLLARS_DATA_MONTH_23",
                                    "DOLLARS_DATA_MONTH_24"],
                "INTDollarColumn": "DOLLARS",
                "requiredFlag": "Y"
            },
            "MandatoryColumnCheck": {
                "STGMandatoryColumn": [
                    "CLIENT_NUMBER", "REPORT_NUMBER",
                    "DDD_SUBCATEGORY_CODE", "SRA3_FACILITY_OUTLET_ASN",
                    "CATEGORY_CODE", "PRODUCT_GROUP", "DATA_DATE",
                    "DOLLARS_DATA_MONTH_1", "DOLLARS_DATA_MONTH_2", "DOLLARS_DATA_MONTH_3",
                    "DOLLARS_DATA_MONTH_4", "DOLLARS_DATA_MONTH_5", "DOLLARS_DATA_MONTH_6",
                    "DOLLARS_DATA_MONTH_7", "DOLLARS_DATA_MONTH_8", "DOLLARS_DATA_MONTH_9",
                    "DOLLARS_DATA_MONTH_10", "DOLLARS_DATA_MONTH_11", "DOLLARS_DATA_MONTH_12",
                    "DOLLARS_DATA_MONTH_13", "DOLLARS_DATA_MONTH_14", "DOLLARS_DATA_MONTH_15",
                    "DOLLARS_DATA_MONTH_16", "DOLLARS_DATA_MONTH_17", "DOLLARS_DATA_MONTH_18",
                    "DOLLARS_DATA_MONTH_19", "DOLLARS_DATA_MONTH_20", "DOLLARS_DATA_MONTH_21",
                    "DOLLARS_DATA_MONTH_22", "DOLLARS_DATA_MONTH_23", "DOLLARS_DATA_MONTH_24",
                    "UNITS_DATA_MONTH_1", "UNITS_DATA_MONTH_2", "UNITS_DATA_MONTH_2",
                    "UNITS_DATA_MONTH_4", "UNITS_DATA_MONTH_5",
                    "UNITS_DATA_MONTH_6", "UNITS_DATA_MONTH_7", "UNITS_DATA_MONTH_8",
                    "UNITS_DATA_MONTH_9", "UNITS_DATA_MONTH_10", "UNITS_DATA_MONTH_11",
                    "UNITS_DATA_MONTH_12", "UNITS_DATA_MONTH_13", "UNITS_DATA_MONTH_14",
                    "UNITS_DATA_MONTH_15", "UNITS_DATA_MONTH_16", "UNITS_DATA_MONTH_17",
                    "UNITS_DATA_MONTH_18", "UNITS_DATA_MONTH_19", "UNITS_DATA_MONTH_20",
                    "UNITS_DATA_MONTH_21", "UNITS_DATA_MONTH_22", "UNITS_DATA_MONTH_23",
                    "UNITS_DATA_MONTH_24"],
                "INTMandatoryColumn": [
                    "CLIENT_NUMBER", "REPORT_NUMBER", "DDD_SUBCATEGORY_CODE",
                    "SRA3_FACILITY_OUTLET_ASN", "SRA_4", "CATEGORY_CODE",
                    "PRODUCT_GROUP", "DATA_DATE", "FILE_INST_ID",
                    "RCD_STAT", "AUDIT_BATCH_ID", "AUDIT_INSRT_DT",
                    "AUDIT_INSRT_NM", "AUDIT_UPDT_NM", "Time_ID",
                    "DOLLARS", "UNIT", "PGN_CODE", "SRC_FCLTY_NUM"
                ]

            }
        }
    }
}

def get_logger(set_level=logging.DEBUG, __module_name__="application_logs"):
    """
    This function is used to create logger object
    :param set_level: setting logger level by default to debug
    :param __module_name__: setting logger module name
    :return: logger object
    """
    logger = logging.getLogger(__module_name__)
    logger.setLevel(set_level)
    log_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    return logger

def data_pivot(df=None, logger=None):
    """
    This function pivots down the raw staging data
    :param df: data frame
    :param logger: logger
    :return: returns pivoted data
    """
    try:
        logger.info("Inside DataIntegration : data_pivot()")
        if df is None:
            raise Exception("data frame is none")
        new_dataframe = df
        new_dataframe = new_dataframe.reset_index()
        dollars_id_value = config['configuration']['SLS_IQVIA']['data_month_mapping'][
            'Dollars_Data']['id_vars']
        dollars_vars_value = config['configuration']['SLS_IQVIA']['data_month_mapping'][
            'Dollars_Data']['value_vars']
        units_id_value = config['configuration']['SLS_IQVIA']['data_month_mapping'][
            'Units_Data']['id_vars']
        units_vars_value = config['configuration']['SLS_IQVIA']['data_month_mapping']['Units_Data'][
            'value_vars']
        time_id_map = config['configuration']['SLS_IQVIA']['data_month_mapping']['Time_ID']
        df1 = pd.melt(new_dataframe, id_vars=dollars_id_value, value_vars=dollars_vars_value,
                      var_name='Time_ID', value_name='DOLLARS', col_level=None)

        df2 = pd.melt(new_dataframe, id_vars=units_id_value, value_vars=units_vars_value,
                      var_name='Unit_Drop', value_name='UNIT', col_level=None)

        cols_to_use = df2.columns.difference(df1.columns)
        pivot_data = df1.merge(df2[cols_to_use], left_index=True, right_index=True, how='outer')
        pivot_data = pivot_data.drop(['Unit_Drop'], axis=1)

        for index in pivot_data.index:
            for keys, values in time_id_map.items():
                if pivot_data.loc[index, 'Time_ID'] == keys:
                    pivot_data.loc[index, 'Time_ID'] = values
        # pivot_data.to_csv("OutputFiles/pivot.csv", index=False)

        return pivot_data

    except Exception as e1:
        error = "Failed to pivot down the data {}".format(str(e1))
        logger.error(error)
        logger.error(str(traceback.print_exc()))
        raise Exception(str(error))
        
def pgn_code(df=None, logger=None):
    """
        This function creates a new column for PGN_CODE
        :param df: data frame
        :param logger: logger
        :return: returns data with PGN_CODE
        """
    try:
        logger.info("Inside DataIntegration : pgn_code()")

        if df is None:
            raise Exception("data frame is none")

        df['PGN_CODE'] = df['PRODUCT_GROUP'].astype(str).str[-3:]

        return df

    except Exception as e2:
        error = "Failed to derive pgn_code  {}".format(str(e2))
        logger.error(error)
        logger.error(str(traceback.print_exc()))
        raise Exception(str(error))


def src_fclty_num(df=None, logger=None):
    """
        This function derives src_fclty_num
        :param df: data frame
        :param logger: logger
        :return: returns data with src_fclty_num
        """
    try:
        logger.info("Inside DataIntegration : src_fclty_num()")

        if df is None:
            raise Exception("data frame is none")

        df.loc[df["SRA3_FACILITY_OUTLET_ASN"].str.startswith("N"), 'SRC_FCLTY_NUM'] = 'I' + df[
            'SRA3_FACILITY_OUTLET_ASN'].astype(str)

        df.loc[~df['SRA3_FACILITY_OUTLET_ASN'].str.startswith("N"), 'SRC_FCLTY_NUM'] = 'UNSP'

        return df

    except Exception as e3:
        error = "Failed to derive src_fclty_num {}".format(str(e3))
        logger.error(error)
        logger.error(str(traceback.print_exc()))
        raise Exception(str(error))


def src_prsbr_id(df=None, logger=None):
    """
        This function derives src_prsbr_id
        :param df: data frame
        :param logger: logger
        :return: returns data with src_prsbr_id
        """
    try:
        logger.info("Inside DataIntegration : src_prsbr_id()")

        if df is None:
            raise Exception("data frame is none")
        df.loc[df['SRA3_FACILITY_OUTLET_ASN'].str.startswith("DOC"), 'SRC_PRSBR_ID'] = \
            df['SRA3_FACILITY_OUTLET_ASN'].str.slice(3, )
        return df

    except Exception as e4:
        error = "Failed to derive rc_prsbr_id {}".format(str(e4))
        logger.error(error)
        logger.error(str(traceback.print_exc()))
        raise Exception(str(error))

if __name__ == "__main__":
    try:        


        # Read recipe inputs
        staging = dataiku.Dataset("STAGING")
        staging_df = staging.get_dataframe()

        logger_obj = get_logger(set_level=logging.DEBUG, __module_name__="DataIntegration")
        pivot_obj = data_pivot(logger=logger_obj, df=staging_df)
        pgn_obj = pgn_code(df=pivot_obj, logger=logger_obj)  
        src_fclty_obj = src_fclty_num(df=pgn_obj, logger=logger_obj)
        src_prsbr_obj = src_prsbr_id(df=src_fclty_obj, logger=logger_obj)
       

        integration_space_eed23760_dku_node_b3d4291b_NVS_df = src_prsbr_obj # For this sample code, simply copy input to output


        # Write recipe outputs
        integration_space_eed23760_dku_node_b3d4291b_NVS = dataiku.Dataset("INTEGRATION_space-eed23760-dku_node-b3d4291b_NVS")
        integration_space_eed23760_dku_node_b3d4291b_NVS.write_with_schema(integration_space_eed23760_dku_node_b3d4291b_NVS_df)
    except Exception as e:
        error1 = "Failed Process the NGE Pipeline {}".format(str(e))
        raise Exception(str(error1))    
