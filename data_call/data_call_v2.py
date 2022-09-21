from utils.df_handle import *

start=time.perf_counter()

# Get dataframe
SRM = pd.read_csv('SRM.csv', dayfirst=True, parse_dates=['FromDate', 'ToDate'])

SLR = pd.read_csv('SLR.csv', dayfirst=True, parse_dates=['StartDate', 'EndDate'])

RD = pd.read_csv('T_RouteDet.csv')
RD.VisitDate = convert_to_datetime(RD.VisitDate)

STL = pd.read_csv('STL.csv')
STL['UpdateTime']=convert_to_datetime(STL['UpdateTime'])

PSO = pd.read_csv('PSO.csv')
PSO.Crtd_DateTime = convert_to_datetime(PSO.Crtd_DateTime)

WCC = pd.read_csv('WithOutNumberCICO.csv')

TS = pd.read_csv('TSlsperID.csv')

U = pd.read_csv('Users.csv')

COM = pd.read_csv('COM.csv')

CU = pd.read_csv('CU.csv')

# Get ET
SRM['SalesRouteID'] = SRM['SalesRouteID'].str.upper()
SLR['SalesRouteID'] = SLR['SalesRouteID'].str.upper()
RD['SalesRouteID'] = RD['SalesRouteID'].str.upper()

T_ExtRoute = RD.merge(SRM.add_prefix('SRM_'), how='inner', left_on=['SalesRouteID'], right_on='SRM_SalesRouteID')
T_ExtRoute = df_filter(T_ExtRoute, DayofWeek='Sun')
T_ExtRoute=T_ExtRoute[\
    (T_ExtRoute['VisitDate'] >= T_ExtRoute['SRM_FromDate']) & \
    (T_ExtRoute['VisitDate'] <= T_ExtRoute['SRM_ToDate'])]

T_ExtRoute = T_ExtRoute.merge(SLR.add_prefix('SLR_'), how='inner', left_on=['SalesRouteID', 'CustID'], right_on=['SLR_SalesRouteID', 'SLR_CustID'])
T_ExtRoute=T_ExtRoute[\
    (T_ExtRoute['VisitDate'] >= T_ExtRoute['SLR_StartDate']) & \
    (T_ExtRoute['VisitDate'] <= T_ExtRoute['SLR_EndDate'])]

T_ExtRoute = dropdup(T_ExtRoute, 1, ['SRM_BranchID', 'CustID', 'SRM_SlsperID', 'SlsFreq', 'SLR_StartDate', 'SLR_EndDate'])
T_ExtRoute = T_ExtRoute[['SRM_BranchID','CustID','SRM_SlsperID','SLR_StartDate','SLR_EndDate']]
T_ExtRoute.columns = ['BranchID', 'CustID', 'SlsperID', 'StartDate','EndDate']
print(T_ExtRoute.shape)

RD.drop('SlsFreq', axis=1, inplace=True)

TResult_1 = RD.merge(SRM.add_prefix('SRM_'), how='inner', left_on=['SalesRouteID'], right_on='SRM_SalesRouteID')
TResult_1=TResult_1[\
    (TResult_1['DayofWeek'] !='Sun') & \
    (TResult_1['VisitDate'] >= TResult_1['SRM_FromDate']) & \
    (TResult_1['VisitDate'] <= TResult_1['SRM_ToDate'])]
lo(6)
TResult_1=TResult_1[['SRM_BranchID', 'SRM_SlsperID', 'SalesRouteID', 'CustID', 'VisitDate']]
TResult_1['InRoute']=1
TResult_1['ExtRoute']=0
TResult_1['Visited']=0
TResult_1['OrderFromPDA']=0
TResult_1['OrdAmtFromPDA']=0
TResult_1['OrderFromOther']=0
TResult_1['OrdAmtFromOther']=0
TResult_1.columns = ['BranchID', 'SlsperID', 'SalesRouteID', 'CustID',
       'VisitDate', 'InRoute', 'ExtRoute', 'Visited', 'OrderFromPDA',
       'OrdAmtFromPDA', 'OrderFromOther', 'OrdAmtFromOther']

df1 = STL.merge(WCC.add_prefix('WCC_'), how='left', left_on=['BranchID', 'NumberCICO'], right_on=['WCC_BranchID','WCC_NumberCICO'])
df2 = df1.merge(SRM.add_prefix('SRM_'), how='inner', left_on=['BranchID','SlsperID'], right_on=['SRM_BranchID','SRM_SlsperID'])
df3=df2[\
    (df2['UpdateTime'] >= df2['SRM_FromDate']) & \
    (df2['UpdateTime'] <= df2['SRM_ToDate'])].copy()


ET = T_ExtRoute
df4 = df3.merge(ET.add_prefix('ET_'), how='left', left_on=['BranchID', 'CustID', 'SlsperID'], right_on=['ET_BranchID', 'ET_CustID', 'ET_SlsperID'])
ctr1 = df4['UpdateTime'] < df4.ET_StartDate
ctr2 = df4['UpdateTime'] > df4.ET_EndDate
list_check = df3.columns.tolist()
df4 = date_between_handler(df4, ctr1, ctr2,'ET_SlsperID', list_check).copy()

TResult_2=df_na(df4, 'WCC_NumberCICO').copy()

TResult_2['InRoute']=0
TResult_2['ExtRoute']=np.where(TResult_2.ET_SlsperID.notna(), 1,0)
TResult_2['Visited']=1
TResult_2['OrderFromPDA']=0
TResult_2['OrdAmtFromPDA']=0
TResult_2['OrderFromOther']=0
TResult_2['OrdAmtFromOther']=0
TResult_2=TResult_2[['BranchID', 'SlsperID', 'SRM_SalesRouteID', 'CustID', 'UpdateTime', 'InRoute', 'ExtRoute', 'Visited', 'OrderFromPDA', 'OrdAmtFromPDA', 'OrderFromOther', 'OrdAmtFromOther']].copy()
TResult_2 = TResult_2.drop_duplicates()
TResult_2.columns=TResult_1.columns

# df3.columns
ET = T_ExtRoute
# df1 = PSO.merge(ET.add_prefix('ET_'), how='left', left_on=['BranchID', 'CustID', 'SlsPerID'], right_on=['ET_BranchID', 'ET_CustID', 'ET_SlsperID'])
df1 = PSO.merge(SLR.add_prefix('SLR_'), how='inner', left_on=['CustID'], right_on=['SLR_CustID'])
ctr1=df1.Crtd_DateTime >= df1['SLR_StartDate']
ctr2=df1.Crtd_DateTime <= df1['SLR_EndDate']
df2=df1[ctr1 & ctr2].copy()

df3 = df2.merge(ET.add_prefix('ET_'), how='left', left_on=['BranchID', 'CustID', 'SlsPerID'], right_on=['ET_BranchID', 'ET_CustID', 'ET_SlsperID'])
ctr1 = df3['Crtd_DateTime'] < df3.ET_StartDate
ctr2 = df3['Crtd_DateTime'] > df3.ET_EndDate
list_check = df2.columns.tolist()
df3 = date_between_handler(df3, ctr1, ctr2, 'ET_BranchID', list_check)
# df3.shape
# df3.columns
df3['InRoute']=0
df3['ExtRoute']=np.where(df3.ET_BranchID.notna(), 1,0)
df3['Visited']=0
df3['OrderFromPDA']=np.where(df3['InsertFrom']=='S', 1, 0)
df3['OrdAmtFromPDA']=np.where(df3['InsertFrom']=='S', df3.LineAmt, 0)
df3['OrderFromOther']=np.where(df3['InsertFrom']!='S', 1, 0)
df3['OrdAmtFromOther']=np.where(df3['InsertFrom']!='S', df3.LineAmt, 0)
# lo(9)
grouplist = ['BranchID', 'SlsPerID', 'SalesRouteID', 'CustID', 'Crtd_DateTime', 'ExtRoute', 'OrderFromPDA', 'OrderFromOther', 'InRoute', 'Visited']
agg_dict = {'OrdAmtFromPDA':np.sum, 'OrdAmtFromOther':np.sum}
TResult_3 = pivot(df3, grouplist, agg_dict)
TResult_3 = TResult_3[['BranchID', 'SlsPerID', 'SalesRouteID', 'CustID', 'Crtd_DateTime', 'InRoute', 'ExtRoute', 'Visited', 'OrderFromPDA', 'OrdAmtFromPDA', 'OrderFromOther', 'OrdAmtFromOther']]
TResult_3.columns = TResult_1.columns

# TResult_3.shape

# TResult_3.ExtRoute.sum()

TResult_4 = union_all([TResult_1, TResult_2])
TResult_4 = union_all([TResult_4, TResult_3])

# TResult_4.dtypes

# df_filter(TResult_4, BranchID='MR0001', SlsperID='MR0499', SalesRouteID='THCM_12', CustID='M0101108', ='A').to_clipboard()

grouplist = ['BranchID', 'SlsperID', 'SalesRouteID', 'CustID', 'VisitDate']
agg_dict = {'InRoute':np.max, 'ExtRoute':np.max, 'Visited':np.max, 'OrderFromPDA':np.max, 'OrdAmtFromPDA':np.max, 'OrderFromOther':np.max, 'OrdAmtFromOther':np.max}
TResult = pivot(TResult_4, grouplist, agg_dict)

df1 = TResult.merge(U.add_prefix('u_'), how='inner', left_on=['SlsperID'], right_on=['u_UserName'])
df2 = df1.merge(TS, how='left', on=['BranchID' , 'SlsperID'])
df3 = df2.merge(U.add_prefix('sup_'), how='left', left_on='SupID', right_on='sup_UserName')
df4 = df3.merge(U.add_prefix('asm_'), how='left', left_on='ASM', right_on='asm_UserName')
df5 = df4.merge(U.add_prefix('rsm_'), how='left', left_on='RSMID', right_on='rsm_UserName')
df6 = df5.merge(COM.add_prefix('com_'), how='inner', left_on='BranchID', right_on='com_CpnyID')
df7 = df6.merge(CU.add_prefix('cu_'), how='inner', left_on='CustID', right_on='cu_CustId')
df7['ExtRoute'] = np.where(df7.InRoute==1, 0, df7['ExtRoute'])
df7['u_Position'] = \
    np.where(df7['u_Position'].isin(['S', 'SS', 'AM', 'RM']), "PBH", \
    np.where(df7['u_Position'].isin(['D', 'SD', 'AD', 'RD']), "MDS", "CS"))

# for l in df7.columns:
#     print(l)



# print(TResult_1.ExtRoute.sum())
# print(TResult_2.ExtRoute.sum())
# print(TResult_3.ExtRoute.sum())

# print(df7.InRoute.sum())
# print(df7.ExtRoute.sum())
# print(df7.Visited.sum())

# df_na(df7, 'SalesRouteID')

cols_list = \
['BranchID',
'com_CpnyName',
'SlsperID',
'SalesRouteID',
'u_FirstName',
'u_Position',
'sup_FirstName',
'asm_FirstName',
'rsm_FirstName',
'CustID',
'cu_CustName',
'cu_Channel',
'cu_ShopType',
'cu_HCOID',
'cu_HCOTypeID',
'cu_ClassId',
'cu_TerritoryDescr',
'cu_StateDescr',
'cu_DistrictDescr',
'cu_WardDescr',
'VisitDate',
'InRoute',
'ExtRoute',
'Visited',
'OrderFromPDA',
'OrdAmtFromPDA',
'OrderFromOther',
'OrdAmtFromOther'
]

final = df7[cols_list].copy()

final.columns = ['BranchID', 'CpnyName', 'SlsperID', 'SalesRouteID', 'SlsperName',
       'Position', 'SupName', 'ASMName', 'RSMName', 'CustID', 'CustName',
       'Channel', 'ShopType', 'HCOID', 'HCOTypeID', 'ClassId',
       'TerritoryDescr', 'StateDescr', 'DistrictDescr', 'WardDescr',
       'VisitDate', 'InRoute', 'ExtRoute', 'Visited', 'OrderFromPDA',
       'OrdAmtFromPDA', 'OrderFromOther', 'OrdAmtFromOther']

primary_keys = ['BranchID', 'SlsperID', 'SalesRouteID', 'CustID', 'VisitDate']

final['inserted_at'] = datetime.now()

final.to_csv('final.csv')

end=time.perf_counter()

print(end-start)


# execute_values_upsert(df1, "f_call_performance", pk=primary_keys)

