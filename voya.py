from numpy import dtype, logical_and
import pyodbc
import config
import pandas as pd

# Connect to eCMS
erp_conn = pyodbc.connect(f'DSN={config.ERP_HOST}; UID={config.ERP_UID}; PWD={config.ERP_PWD}')
sql = """
        SELECT 
        MST.PRTMSTID,
        CASE WHEN MST.EMPLOYEENO>0 THEN 'INGWIN8' END AS "RECORD_TYPE",
        CASE WHEN MST.EMPLOYEENO>0 THEN 771202 END AS "EMPLOYERID",
        CASE WHEN MST.EMPLOYEENO>0 THEN 4 END AS "CYCLE",
        CAST(MST.SOCIALSECNO AS VARCHAR(9)),
        CASE WHEN MST.COMPANYNO = 1 THEN '0001' WHEN MST.COMPANYNO = 30 THEN '0002' END AS "LOCATION_CODE",
        TRIM(MST.LASTEMPNAME),
        TRIM(MST.FIRSTEMPNAME), 
        LEFT(TRIM(MST.MIDDLENAME1), 1), 
        TRIM(MST.ADDR1), 
        TRIM(MST.ADDR2), 
        TRIM(MST.CITY), 
        MST.STATECODE, 
        TRIM(MST.ZIPCODE),
        CASE WHEN MST.STATUSCODE = 'I' THEN 'T' WHEN MST.STATUSCODE = 'A' THEN 'A' END AS "EMPLOYEE_STATUS_CODE",
        CASE WHEN MST.BIRTHDATE IS NULL THEN '0' ELSE CAST(MST.BIRTHDATE AS INT) END, 
        CASE WHEN MST.ORIGHIREDATE IS NULL THEN '0' ELSE CAST(MST.ORIGHIREDATE AS INT) END, 
        CASE WHEN MST.TERMDATE IS NULL THEN '0' ELSE CAST(MST.TERMDATE AS INT) END, 
        CASE WHEN MST.ADJHIREDATE IS NULL THEN '0' ELSE CAST(MST.ADJHIREDATE AS INT) END,   
        MST.EMPLOYEENO,
        HST.CHECKDATE,
        HST.REGHRS + HST.OVTHRS + HST.OTHHRS,
        MED.DEDUCTIONAMT,
        CAST(MED.DEDNUMBER AS VARCHAR(3))
        FROM CMSFIL.PRTHST AS HST
        JOIN CMSFIL.PRTMST AS MST
            ON (
                HST.EMPLOYEENO=MST.EMPLOYEENO and HST.COMPANYNO=MST.COMPANYNO AND MST.EMPLOYEENO = 10703
            )
        JOIN CMSFIL.PRTMED AS MED
            ON (
                MED.CHECKDATE=HST.CHECKDATE AND MED.EMPLOYEENO=HST.EMPLOYEENO
            )
        WHERE MST.COMPANYNO IN (1,30) AND CAST(HST.CHECKDATE AS INT) > 111111
        AND MED.DEDNUMBER IN (401, 402, 410, 411)
        """

# Quarterly Query
quarter_filter_input = '2021Q1'
if not quarter_filter_input:
    quarter_filter_input = input('Enter a Year and Period(2020Q1): ')

# Write SQL Return to datafra,e
data = pd.read_sql(sql, erp_conn)

# Rename all Columns
columns = [
    'PRTMSTID', 'RecordType', 'EmployerID', 'PayrollCycle', 'SSN',
    'LocationCode', 'LastName', 'FirstName', 'MI', 'Addr1', 'Addr2',
    'City', 'State', 'Zip', 'EmployeeStatusCode', 'BirthDate', 'HireDate',
    'TermDate', 'AdjHireDate', 'EmployeeNo', 'CheckDate', 'TotHrs', 
    'DedAmt', 'DedNo',
    ]
data.columns=columns

# Create Quarterly Periods from CheckDate
data['Period'] = pd.PeriodIndex(pd.to_datetime(data['CheckDate']), freq='Q')

group = [
    'PRTMSTID', 'RecordType', 'EmployerID', 'PayrollCycle', 'SSN', 'LocationCode',
    'LastName', 'FirstName', 'MI', 'Addr1', 'Addr2', 'City', 'State', 'Zip',
    'EmployeeStatusCode', 'BirthDate', 'HireDate', 'TermDate', 'AdjHireDate',
    'EmployeeNo', 'Period'
    ]

# Filter main set by quarter
quarter_filter = data['Period'] == quarter_filter_input
voya_df = data[quarter_filter]

# Ensure that necessary columns are strings
voya_df['SSN'] = voya_df['SSN'].astype('str').apply(lambda x: '{0:0>9}'.format(x))

# Create subset that has deductions per quarter from main filtered set and add column to main df
data_subset = voya_df[['PRTMSTID', 'Period', 'DedNo', 'DedAmt']]
deduction_subset = data_subset.groupby(['PRTMSTID', 'Period', 'DedNo']).transform('sum')
voya_df['ContributionAmount'] = deduction_subset

# Create a subset that has hours per quarter from main filtered set and add column to main df
data_subset = voya_df[['PRTMSTID', 'Period', 'DedNo', 'TotHrs']]
data_subset['SubTotal'] = data_subset.groupby(['PRTMSTID', 'Period', 'DedNo', ]).transform('sum')

# Get max value of hours for deduction per quarter (giving total hours worked) and add column to main df
data_subset = data_subset[['PRTMSTID', 'Period', 'SubTotal']]
voya_df['PeriodHours'] = data_subset.groupby(['PRTMSTID', 'Period']).transform('max')

# Remove extra columns that add unneeded detail
del voya_df['DedAmt']
del voya_df['CheckDate']
del voya_df['TotHrs']

# Drop duplicates
voya_df = voya_df.drop_duplicates()

# Refactor to Match Win8 Voya Output
# Add Empty Columns
voya_df['Zip2'] = ''
voya_df['YTDHours'] = ''
voya_df['AnnHours'] = ''
voya_df['SourceCode1'] = ''
voya_df['ContAmount1'] = 0
voya_df['SourceCode2'] = ''
voya_df['ContAmount2'] = 0
voya_df['SourceCode3'] = ''
voya_df['ContAmount3'] = 0
voya_df['SourceCode4'] = ''
voya_df['ContAmount4'] = 0
voya_df['SourceCode5'] = ''
voya_df['ContAmount5'] = 0
voya_df['SourceCode6'] = ''
voya_df['ContAmount6'] = 0
voya_df['LoanNumber1'] = ''
voya_df['LoanPmt1'] = 0
voya_df['LoanNumber2'] = ''
voya_df['LoanPmt2'] = 0
voya_df['LoanNumber3'] = ''
voya_df['LoanPmt3'] = 0
voya_df['LoanNumber4'] = ''
voya_df['LoanPmt4'] = 0
voya_df['LoanNumber5'] = ''
voya_df['LoanPmt5'] = 0
voya_df['LoanNumber6'] = ''
voya_df['LoanPmt6'] = 0
voya_df['Reserved'] = ''
voya_df['EmpEligibility'] = ''

# Re-order dataframe to match specifics
win38_output = [
    'PRTMSTID', 'RecordType', 'EmployerID', 'PayrollCycle', 'Period', 'SSN', 'LocationCode', 'LastName', 'FirstName', 'MI', 'Addr1', 
    'Addr2', 'City', 'State', 'Zip', 'Zip2', 'EmployeeStatusCode', 'BirthDate', 'HireDate', 'TermDate', 'AdjHireDate', 'YTDHours', 
    'PeriodHours', 'AnnHours', 'DedNo', 'ContributionAmount', 'SourceCode1', 'ContAmount1', 'SourceCode2', 'ContAmount2', 
    'SourceCode3', 'ContAmount3', 'SourceCode4', 'ContAmount4', 'SourceCode5', 'ContAmount5', 'SourceCode6', 'ContAmount6', 'LoanNumber1',
    'LoanPmt1', 'LoanNumber2', 'LoanPmt2', 'LoanNumber3', 'LoanPmt3', 'LoanNumber4', 'LoanPmt4', 'LoanNumber5', 'LoanPmt5', 'LoanNumber6', 
    'LoanPmt6', 'Reserved', 'EmpEligibility', 'EmployeeNo', ]

# Update dataframe to have win38 columns    
voya_df = voya_df[win38_output]

# Fill appropriate columns with static data
voya_df['SourceCode1'] = '401'
voya_df['SourceCode2'] = '402'
voya_df['LoanNumber1'] = '410'
voya_df['LoanNumber2'] = '411'

# Take contribution amounts of each deduction and add then to their respective columns
voya_df['ContAmount1'] = round(voya_df.loc[voya_df['DedNo'] == '401']['ContributionAmount'], 2)
voya_df['ContAmount2'] = round(voya_df.loc[voya_df['DedNo'] == '402']['ContributionAmount'], 2)
voya_df['LoanPmt1'] = round(voya_df.loc[voya_df['DedNo'] == '410']['ContributionAmount'], 2)
voya_df['LoanPmt2'] = round(voya_df.loc[voya_df['DedNo'] == '411']['ContributionAmount'], 2)

# Delete extra detail columns
del voya_df['DedNo']
del voya_df['ContributionAmount']

# Create subset that has Contribution Amount 1 per quarter from main filtered set
data_subset = voya_df[['PRTMSTID', 'Period', 'ContAmount1']]
const1_subset = data_subset.groupby(['PRTMSTID', 'Period']).transform('sum')
voya_df['ContAmount1'] = const1_subset

# Create subset that has Contribution Amount 2 per quarter from main filtered set
data_subset = voya_df[['PRTMSTID', 'Period', 'ContAmount2']]
const2_subset = data_subset.groupby(['PRTMSTID', 'Period']).transform('sum')
voya_df['ContAmount2'] = const2_subset

# Create subset that has LoanPmt1  per quarter from main filtered set
data_subset = voya_df[['PRTMSTID', 'Period', 'LoanPmt1']]
LoanPmt1 = data_subset.groupby(['PRTMSTID', 'Period']).transform('sum')
voya_df['LoanPmt1'] = LoanPmt1

# Create subset that has LoanPmt2  per quarter from main filtered set
data_subset = voya_df[['PRTMSTID', 'Period', 'LoanPmt2']]
LoanPmt2 = data_subset.groupby(['PRTMSTID', 'Period']).transform('sum')
voya_df['LoanPmt2'] = LoanPmt2

# Delete duplicate rows
voya_df = voya_df.drop_duplicates()

# Finalize
print(voya_df)
voya_df.to_csv('voya.csv', sep=',', encoding='utf-8')