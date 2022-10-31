# from utils.df_handle import *

def update_sync_dms_orddisc():

    from_tb = "OM_OrdDisc"
    table_name = "sync_dms_omorddics"
    # table_temp = "sync_dms_omorddics_temp"

    sql =\
    f"""
    select
    BranchID, 
    --sq.DiscID, 
    --sq.DiscSeq, 
    sq.Descr, 
    sq.DiscIDPN,
    sq.TypeDiscount,
    sq.AccumulateID,
    OrderNbr, 
    --LineRef, 
    --SOLineRef, 
    case when GroupRefLineRef = '' then SOLineRef else GroupRefLineRef end as GroupRefLineRef,
    cast(dis.Crtd_DateTime as date) as Crtd_DateTime,
    cast(dis.LUpd_DateTime as date) as LUpd_DateTime
    from {from_tb} dis
    INNER JOIN dbo.OM_DiscSeq sq WITH (NOLOCK)
    ON sq.DiscID = dis.DiscID
    AND sq.DiscSeq = dis.DiscSeq
    where CAST(dis.Crtd_DateTime as date) <= '{datenow_mns45}'
    """
    df = get_ms_df(sql)
    df.columns = lower_col(df)
    df = pd.concat([df.groupreflineref.str.split(",", expand=True), df], axis=1)
    df_aut = pd.melt(df,id_vars=['branchid','descr', 'discidpn', 'typediscount', 'accumulateid', 'ordernbr', 'groupreflineref', 'crtd_datetime', 'lupd_datetime'], value_name='glineref')
    dk = df_aut['glineref'].notna()
    df=df_aut[dk].copy()
    df['pk'] = df.branchid.astype(str)+"-"+df.ordernbr.astype(str)+"-"+df.glineref.astype(str)
    df['pk2'] = df.branchid.astype(str)+"-"+df.ordernbr.astype(str)+"-"+df.glineref.astype(str)+"-"+df.discidpn.astype(str)
    df = dropdup(df,1, ['pk2'])
    groups=df.groupby(['pk','branchid', 'ordernbr', 'glineref', 'crtd_datetime', 'lupd_datetime'])
    df3=groups['discidpn'].apply(list).reset_index(name='discidpn')
    assert checkdup(df3,2, 'pk').sum() == 0,"Duplicate"
    df3['discidpn1'] = df3['discidpn'].apply(lambda x:x[0])
    df3['discidpn2'] = df3['discidpn'].apply(lambda x:x[1] if(len(x) > 1) else None)
    df3['discidpn3'] = df3['discidpn'].apply(lambda x:x[2] if(len(x) > 2) else None)
    df4=groups['descr'].apply(list).reset_index(name='descr')
    df4['descr1'] = df4['descr'].apply(lambda x:x[0])
    df4['descr2'] = df4['descr'].apply(lambda x:x[1] if(len(x) > 1) else None)
    df4['descr3'] = df4['descr'].apply(lambda x:x[2] if(len(x) > 2) else None)
    df4=df4[['descr','descr1','descr2','descr3']]
    df5=groups['typediscount'].apply(list).reset_index(name='typediscount')
    df5['typediscount1'] = df5['typediscount'].apply(lambda x:x[0])
    df5['typediscount2'] = df5['typediscount'].apply(lambda x:x[1] if(len(x) > 1) else None)
    df5['typediscount3'] = df5['typediscount'].apply(lambda x:x[2] if(len(x) > 2) else None)
    df5=df5[['typediscount','typediscount1','typediscount2','typediscount3']]
    df6 = pd.concat([df3,df4], axis=1)
    df6 = pd.concat([df6,df5], axis=1)
    df6['inserted_at'] = datetime.now()
    del_sql = \
    f"""
    delete from biteam.{table_name} where date(crtd_datetime) >= '{datenow_mns45}'
    """
    print("delete_sql: ", del_sql)
    execute_bq_query(del_sql)
    # df6.shape
    bq_values_insert(df6, table_name, 2, chunksize=None)