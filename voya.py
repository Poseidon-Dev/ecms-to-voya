import pyodbc
import config
import pandas as pd

erp_conn = pyodbc.connect(f'DSN={config.ERP_HOST}; UID={config.ERP_UID}; PWD={config.ERP_PWD}')
sql = """
        SELECT 
        MST.PRTMSTID,
        CASE WHEN MST.EMPLOYEENO>0 THEN 'INGWIN8' END AS "RECORD_TYPE",
        CASE WHEN MST.EMPLOYEENO>0 THEN 771202 END AS "EMPLOYERID",
        CASE WHEN MST.EMPLOYEENO>0 THEN 4 END AS "CYCLE", 
        CAST(MST.SOCIALSECNO AS INT),
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
                HST.EMPLOYEENO=MST.EMPLOYEENO and HST.COMPANYNO=MST.COMPANYNO
            )
        JOIN CMSFIL.PRTMED AS MED
            ON (
                MED.CHECKDATE=HST.CHECKDATE AND MED.EMPLOYEENO=HST.EMPLOYEENO
            )
        WHERE MST.COMPANYNO IN (1,30) AND CAST(HST.CHECKDATE AS INT) > 111111
        AND MED.DEDNUMBER IN (401, 402, 410, 411)
        """

quarter_filter_input = input('Enter a Year and Quarter(2020Q1): ')

data = pd.read_sql(sql, erp_conn)
columns = [
    'PRTMSTID', 'Record_Type', 'EmployerID', 'Cycle', 'SSN',
    'Location_Code', 'LastName', 'FirstName', 'MI', 'Addr1', 'Addr2',
    'City', 'State', 'Zip', 'Employee_Status', 'Birthdate', 'Hire_Date',
    'Term_Date', 'Adj_Hire_Date', 'EmployeeNo', 'Check_Date', 'TotHrs', 
    'Ded_Amt', 'Ded_no',
    ]
group = [
    'PRTMSTID', 'Record_Type', 'EmployerID', 'Cycle', 'SSN', 'Location_Code',
    'LastName', 'FirstName', 'MI', 'Addr1', 'Addr2', 'City', 'State', 'Zip',
    'Employee_Status', 'Birthdate', 'Hire_Date', 'Term_Date', 'Adj_Hire_Date',
    'EmployeeNo', 'Quarter'
    ]

data.columns=columns
data['Quarter'] = pd.PeriodIndex(pd.to_datetime(data['Check_Date']), freq='Q')

# Filter main set by quarter
quarter_filter = data['Quarter'] == quarter_filter_input
data_test = data[quarter_filter]

# Create subset that has deductions per quarter from main filtered set
data_subset = data_test[['PRTMSTID', 'Quarter', 'Ded_no', 'Ded_Amt']]
deduction_subset = data_subset.groupby(['PRTMSTID', 'Quarter', 'Ded_no']).transform('sum')

# Add deduction amounts up per person per quarter and add column to main filtered set
data_test['DeductionAmount'] = deduction_subset

# Create a subset that has hours per quarter from main filtered set
data_subset = data_test[['PRTMSTID', 'Quarter', 'Ded_no', 'TotHrs']]

# Get total hours for each deduction per employee
data_subset['SubTotal'] = data_subset.groupby(['PRTMSTID', 'Quarter', 'Ded_no', ]).transform('sum')

# Get max value of hours for deduction per quarter (giving total hours worked)
data_subset = data_subset[['PRTMSTID', 'Quarter', 'SubTotal']]
data_test['PeriodHours'] = data_subset.groupby(['PRTMSTID', 'Quarter']).transform('max')

# Remove extra columns that add unneeded detail
del data_test['Ded_Amt']
del data_test['Check_Date']
del data_test['TotHrs']

# Drop duplicates
data_test = data_test.drop_duplicates()

# Wide to long main filtered dataset to move deduction numbers to own columns with deduction amount as value
index = ['PRTMSTID', 'Record_Type', 'EmployerID', 'Cycle', 'SSN', 'Location_Code', 'LastName', 'FirstName', 'MI',
'Addr1', 'Addr2', 'City', 'State', 'Zip', 'Employee_Status', 'Birthdate', 'Hire_Date', 'Term_Date', 'Adj_Hire_Date',
'EmployeeNo', 'Quarter', 'PeriodHours']
data_test = data_test.pivot_table(index=index, columns='Ded_no', values='DeductionAmount', fill_value=0)

# Fill NaN with 0
print(data_test.head(50))
