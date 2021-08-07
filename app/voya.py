import pyodbc
import app.config
import pandas as pd

from datetime import date, timedelta

def date_to_int(date):
    date_format = '%Y%m%d'
    return int(date.strftime(date_format))

def collect_voya_data():
    
    # Set to run on a Monday to collect previous Saturday through Tuesday
    transmission_year = date(date.today().year, 1, 1)
    transmission_end_date = date.today() - timedelta(2)
    transmission_start_date = transmission_end_date - timedelta(4)

    weeks_in_year = abs(transmission_end_date - transmission_year).days//7

    transmission_year = date_to_int(transmission_year)
    transmission_end_date = date_to_int(transmission_end_date)
    transmission_start_date = date_to_int(transmission_start_date)

    
    # Connect to eCMS
    erp_conn = pyodbc.connect(f'DSN={app.config.ERP_HOST}; UID={app.config.ERP_UID}; PWD={app.config.ERP_PWD}')
    sql = f"""
            SELECT 
            MST.PRTMSTID,
            CASE WHEN MST.EMPLOYEENO>0 THEN 'INGWIN8' END AS "RECORD_TYPE",
            CASE WHEN MST.EMPLOYEENO>0 THEN 771202 END AS "EMPLOYERID",
            CASE WHEN MST.EMPLOYEENO>0 THEN 7 END AS "CYCLE",
            LEFT(MST.SOCIALSECNO, 3),
            CASE WHEN MST.COMPANYNO = 1 THEN 1 WHEN MST.COMPANYNO = 30 THEN 2 END AS "LOCATION_CODE",
            TRIM(MST.LASTEMPNAME),
            TRIM(MST.FIRSTEMPNAME),
            LEFT(TRIM(MST.MIDDLENAME1), 1),
            TRIM(MST.ADDR1),
            TRIM(MST.ADDR2),
            TRIM(MST.CITY),
            MST.STATECODE,
            TRIM(MST.ZIPCODE),
            CASE WHEN MST.STATUSCODE = 'I' THEN 'T' WHEN MST.STATUSCODE = 'A' THEN 'A' END AS "EMPLOYEE_STATUS_CODE",
            CASE WHEN MST.BIRTHDATE IS NULL THEN '' ELSE CAST(MST.BIRTHDATE AS INT) END,
            CASE WHEN MST.ORIGHIREDATE IS NULL THEN '' ELSE CAST(MST.ORIGHIREDATE AS INT) END,
            CASE WHEN MST.TERMDATE IS NULL THEN '' ELSE CAST(MST.TERMDATE AS INT) END,
            CASE WHEN MST.ADJHIREDATE IS NULL THEN '' ELSE CAST(MST.ADJHIREDATE AS INT) END,
            MST.EMPLOYEENO,
            CAST(HST.CHECKDATE AS INT),
            HST.REGHRS + HST.OVTHRS + HST.OTHHRS END,
            MED.DEDUCTIONAMT,
            CAST(MED.DEDNUMBER AS VARCHAR(3)),
            CASE 
                WHEN MST.STDDEPTNO = 47 THEN 'N'
                WHEN RIGHT(TRIM(CAST(MST.EMPLTYPE AS VARCHAR(4))), 1 ) = 'P' THEN 'N'
                ELSE 'Y' END,
            MST.PAYTYPE
            FROM CMSFIL.PRTHST AS HST
            JOIN CMSFIL.PRTMST AS MST
                ON (
                    HST.EMPLOYEENO=MST.EMPLOYEENO and HST.COMPANYNO=MST.COMPANYNO
                )
            JOIN CMSFIL.PRTMED AS MED
                ON (
                    MED.CHECKDATE=HST.CHECKDATE AND MED.EMPLOYEENO=HST.EMPLOYEENO
                )
            WHERE MST.COMPANYNO IN (1,30) 
            AND CAST(HST.CHECKDATE AS INT) between {transmission_year} AND {transmission_end_date}
            AND MED.DEDNUMBER IN (401, 402, 410, 411)
            """

    # Write SQL Return to datafra,e
    voya_df = pd.read_sql(sql, erp_conn)

    # Rename all Columns
    columns = [
        'PRTMSTID', 'RecordType', 'EmployerID', 'PayrollCycle', 'SSN',
        'LocationCode', 'LastName', 'FirstName', 'MI', 'Addr1', 'Addr2',
        'City', 'State', 'Zip', 'EmployeeStatusCode', 'BirthDate', 'HireDate',
        'TermDate', 'AdjHireDate', 'EmployeeNo', 'Period', 'TotHrs', 
        'DedAmt', 'DedNo', 'EmpEligibility', 'PayType'
        ]
    voya_df.columns=columns

    def none_or_sum(value):
        sum_value = value.sum()
        if sum_value == 0:
            sum_value = ''
        return sum_value

    def none_or_max(value):
        max_value = value.max()
        if max_value == 0:
            max_value = ''
        return max_value


    # Create subset that has deductions per period from main filtered set and add column to main df
    data_subset = voya_df[['PRTMSTID', 'Period', 'DedNo', 'DedAmt']]
    deduction_subset = data_subset.groupby(['PRTMSTID', 'Period', 'DedNo']).transform('sum')
    voya_df.loc[:, 'ContributionAmount'] = deduction_subset

    # Create a subset that has hours per period from main filtered set and add column to main df
    data_subset = voya_df[['PRTMSTID', 'Period', 'DedNo', 'TotHrs']]
    data_subset.loc[:, 'SubTotal'] = data_subset.groupby(['PRTMSTID', 'Period', 'DedNo', ]).transform(none_or_sum)

    # Get max value of hours for deduction per period (giving total hours worked) and add column to main df
    data_subset = data_subset[['PRTMSTID', 'Period', 'SubTotal']]
    voya_df.loc[:, 'PeriodHours'] = data_subset.groupby(['PRTMSTID', 'Period']).transform(none_or_max)

    # Get annual hours for specified period and add column to main df
    data_subset = voya_df[['PRTMSTID', 'TotHrs']]
    voya_df.loc[:, 'YTDHours'] = data_subset.groupby(['PRTMSTID']).transform('sum')

    # Makes salaried a flat amount based on weeks within year
    voya_df.loc[voya_df['PayType'] == 'S', 'YTDHours'] = weeks_in_year * 40


    # Remove extra columns that add unneeded detail
    del voya_df['DedAmt']
    del voya_df['TotHrs']

    # Drop duplicates
    voya_df = voya_df.drop_duplicates()

    # Refactor to Match Win8 Voya Output
    # Add Empty Columns
    voya_df.loc[:, 'Zip2'] = ''
    voya_df.loc[:, 'AnnHours'] = ''
    voya_df.loc[:, 'SourceCode1'] = 'A'
    voya_df.loc[:, 'ContAmount1'] = ''
    voya_df.loc[:, 'SourceCode2'] = ''
    voya_df.loc[:, 'ContAmount2'] = ''
    voya_df.loc[:, 'SourceCode3'] = ''
    voya_df.loc[:, 'ContAmount3'] = ''
    voya_df.loc[:, 'SourceCode4'] = ''
    voya_df.loc[:, 'ContAmount4'] = ''
    voya_df.loc[:, 'SourceCode5'] = ''
    voya_df.loc[:, 'ContAmount5'] = ''
    voya_df.loc[:, 'SourceCode6'] = ''
    voya_df.loc[:, 'ContAmount6'] = ''
    voya_df.loc[:, 'LoanNumber1'] = ''
    voya_df.loc[:, 'LoanPmt1'] = ''
    voya_df.loc[:, 'LoanNumber2'] = ''
    voya_df.loc[:, 'LoanPmt2'] = ''
    voya_df.loc[:, 'LoanNumber3'] = ''
    voya_df.loc[:, 'LoanPmt3'] = ''
    voya_df.loc[:, 'LoanNumber4'] = ''
    voya_df.loc[:, 'LoanPmt4'] = ''
    voya_df.loc[:, 'LoanNumber5'] = ''
    voya_df.loc[:, 'LoanPmt5'] = ''
    voya_df.loc[:, 'LoanNumber6'] = ''
    voya_df.loc[:, 'LoanPmt6'] = ''
    voya_df.loc[:, 'Reserved'] = ''

    # Re-order dataframe to match specifics
    win38_output = [
        'PRTMSTID', 'RecordType', 'EmployerID', 'PayrollCycle', 'Period', 'SSN', 'LocationCode', 'LastName', 'FirstName', 'MI', 'Addr1', 
        'Addr2', 'City', 'State', 'Zip', 'Zip2', 'EmployeeStatusCode', 'BirthDate', 'HireDate', 'TermDate', 'AdjHireDate', 'YTDHours', 
        'PeriodHours', 'AnnHours', 'DedNo', 'ContributionAmount', 'SourceCode1', 'ContAmount1', 'SourceCode2', 'ContAmount2', 
        'SourceCode3', 'ContAmount3', 'SourceCode4', 'ContAmount4', 'SourceCode5', 'ContAmount5', 'SourceCode6', 'ContAmount6', 'LoanNumber1',
        'LoanPmt1', 'LoanNumber2', 'LoanPmt2', 'LoanNumber3', 'LoanPmt3', 'LoanNumber4', 'LoanPmt4', 'LoanNumber5', 'LoanPmt5', 'LoanNumber6', 
        'LoanPmt6', 'Reserved', 'EmpEligibility', 'EmployeeNo', 'PayType' ]

    # Update dataframe to have win38 columns    
    voya_df = voya_df[win38_output]

    # Take contribution amounts of each deduction and add then to their respective columns
    voya_df.loc[:,'ContAmount1'] = round(voya_df.loc[voya_df['DedNo'].isin(['401', '402'])]['ContributionAmount'],2)
    voya_df.loc[:,'LoanPmt1'] = round(voya_df.loc[voya_df['DedNo'].isin(['410', '411'])]['ContributionAmount'], 2)

    # Delete extra detail columns
    del voya_df['DedNo']
    del voya_df['ContributionAmount']
    del voya_df['PayType']
    # Create subset that has Contribution Amount 1 per period from main filtered set
    data_subset = voya_df[['PRTMSTID', 'Period', 'ContAmount1']]
    const1_subset = data_subset.groupby(['PRTMSTID', 'Period']).transform('sum')
    voya_df.loc[:,'ContAmount1'] = const1_subset

    # Create subset that has LoanPmt1  per period from main filtered set
    data_subset = voya_df[['PRTMSTID', 'Period', 'LoanPmt1']]
    LoanPmt1 = data_subset.groupby(['PRTMSTID', 'Period']).transform('sum')
    voya_df.loc[:,'LoanPmt1'] = LoanPmt1

    # Delete duplicate rows
    voya_df = voya_df.drop_duplicates()

    # Keep relevant rows
    voya_final = voya_df[voya_df['Period'].between(transmission_start_date, transmission_end_date)]

    # Drop Internal Key
    del voya_final['PRTMSTID']

    # Finalize
    file_name = f'{transmission_end_date}.csv'
    voya_final.to_csv(f'dumps/{file_name}', float_format='%.2f', sep=',', encoding='utf-8', index=False)
    print('Collected Data')
    return file_name
