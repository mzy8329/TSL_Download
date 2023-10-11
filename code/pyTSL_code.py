import pyTSL
from datetime import datetime
import pandas as pd


args_dict = {
    'date': "datetimetostr(['date']) as 'time'",
    'StockID': "['StockID'] as 'symbol_id'",
    'StockName': "['StockName'] as 'symbol_name'",
    'price': "['price'] as 'last'",
    'vol': "['vol'] as 'trade_volume'",
    'amount': "['amount'] as 'trade_amount'",
    'cjbs': "['cjbs'] as 'open_interest_change'",
    'bc1': "['bc1'] as 'bv1'",
    'buy1': "['buy1'] as 'bp1'",
    'sale1': "['sale1'] as 'ap1'",
    'sc1': "['sc1'] as 'av1'",
    'sectional yclose': "['sectional_yclose'] as 'yesterday_close'",
    'sectional_open': "['sectional_open'] as 'today_open'",
    'sectional_high': "['sectional_high'] as 'today_high'",
    'sectional_low': "['sectional_low'] as 'today_low'",
    'sectional_vol': "['sectional_vol'] as 'volume'",
    'sectional_amount': "['sectional_amount'] as 'amount'",
    'sectional_cjbs': "['sectional_cjbs'] as 'open_interest'",
    'open': "['open']  as 'open'",                          # for k only
    'high': "['high']  as 'high'",                          # for k only
    'low': "['low']  as 'low'",                             # for k only
    'yclose': "['yclose']  as 'close'",                     # for k only
    'Syl2': "['Syl2']  as 'yesterday_settlementprice'",      # for k only
    '合约代码': 703001,
    '变动日': 703002,
    '交易代码':  703003,
    '交割年份':	 703004,
    '交割月份':	 703005,
    '交易品种':	 703006,
    '合约乘数':	 703007,
    '合约乘数单位': 703008,
    '报价单位': 703009,
    '最小变动价位': 703010,
    '每日价格最大波动下限(%)': 703011,
    '每日价格最大波动上限(%)':	 703012,
    '最后交易日参照标准': 703013,
    '最后交易日相对参照标准偏移月份': 703014,
    '最后交易日类别': 703015,
    '最后交易日相对最后交易日所在月份偏移天数': 703016,
    '最后交易日是否假日顺延': 703017,
    '最后交易日': 703018,
    '最后交割日参照标准': 703019,
    '最后交割日相对参照标准偏移月份': 703020,
    '最后交割日类别': 703021,
    '最后交割日相对最后交割日所在月份偏移天数': 703022,
    '最后交割日是否假日顺延': 703023,
    '最后交割日': 703024,
    '最低交易保证金(%)': 703025,
    '交割方式': 703026,
    '上市地': 703027,
    '期货类别': 703028,
    '商品期货类别': 703029,
    '基准代码': 703030
}

tick_args = [
    'date', 'StockID', 'StockName', 'price', 'vol', 'amount', 'cjbs', 'bc1', 'buy1', 'sale1', 'sc1', 'sectional yclose',
    'sectional_open', 'sectional_high', 'sectional_low', 'sectional_vol', 'sectional_amount', 'sectional_cjbs'
]

k_args = [
    'date', 'StockID', 'StockName', 'price', 'vol', 'amount', 
    'cjbs', 'open', 'high', 'low', 'yclose', 'Syl2',
    'sectional yclose', 'sectional_open', 'sectional_high', 'sectional_low', 'sectional_vol', 'sectional_amount', 'sectional_cjbs'
]

future_args = [ 
    '合约代码',
    '变动日',	
    '交易代码',	
    '交割年份',	
    '交割月份',	
    '交易品种',	
    '合约乘数',	
    '合约乘数单位',
    '报价单位',
    '最小变动价位',
    '每日价格最大波动下限(%)',
    '每日价格最大波动上限(%)',	
    '最后交易日参照标准',
    '最后交易日相对参照标准偏移月份',
    '最后交易日类别',
    '最后交易日相对最后交易日所在月份偏移天数',
    '最后交易日是否假日顺延',
    '最后交易日',
    '最后交割日参照标准',
    '最后交割日相对参照标准偏移月份',
    '最后交割日类别',
    '最后交割日相对最后交割日所在月份偏移天数',
    '最后交割日是否假日顺延',
    '最后交割日',
    '最低交易保证金(%)',
    '交割方式',
    '上市地',
    '期货类别',
    '商品期货类别',
    '基准代码'
]


# 需要根据所需路径进行更改
path = {
    'data_save': '/home/mzy/Documents/Project/9_25韩老师数据下载/data/'
}

def TS_INIT():
    c = pyTSL.Client("hit", "20230427", "tsl.tinysoft.com.cn", 443)
    c.login()
    return c


def write_file(df, name):
    output_path = path['data_save'] + name
    df= pd.DataFrame(df)
    df.to_csv(output_path)
    print(df.shape, output_path)
    return 1
pyTSL.register_proc('write_file', write_file)


def code_get_bk_list(bk_name=''):
    if bk_name == '':
        code = '''
            return getBKList2();
            '''
    else:
        # 层级结构不好用，还没找到原因
        code = '''
            return getBKList2('%s');
            ''' % bk_name
    return code


def code_get_bk(bk_name):
    code = '''
        return getbk('%s');
        ''' % bk_name
    return code

def code_OP_GetOptionChain(pz, Endt):
    code = '''
        PZ:= '%s';
        Endt:= %s;
        return OP_GetOptionChain(PZ, Endt);
        ''' % (pz, Endt)
    return code

def code_get_data(begT, endT, args, bk_name = '', stock_name = '', time_cycle = ''):
    """
    获取并返回TSL语言代码, 执行该代码可直接下载某板块中的所有股票或某个股票的数据(k线或tick线)
    :param begT: 数据起始时间(包括该日数据)
    :param endT: 数据终止时间(包括该日数据)
    :param bk_name: 板块名称(可为空), 若板块名称不为空，下载该板块下的所有股票， 
    :param stock_name: 股票名称(可为空), 若股票名称不为空且板块名称为空，则下载该股票的内容
    :param time_cycle: 行情频率(可为空), 所需数据为tick线时, 设定为空, 所需数据为k线时, 设置为所需的频率
    """
    arg_string = ''
    for arg in args:
        arg_string += args_dict[arg]
        if arg != args[-1]:
            arg_string += ','

    if time_cycle != '':
        set_time_cycle_string = 'setsysparam(pn_cycle(), %s);'% time_cycle
        table = 'markettable'
        type_string = 'k'
    else:
        set_time_cycle_string = ''
        table = 'tradetable'
        type_string = 'tick'

    if bk_name != '':
        code = '''
            begT := %s;
            endT := %s;
            bkArr:= getbk('%s');
            
            Tarr := MarketTradeDayQk(begT, endT);        
            for t_index:=0 to length(Tarr)-1 do
            begin
                %s
                bk :=   
                    sselect thisrow 
                    from bkArr
                    where spec(FuturesIsTrade(Tarr[t_index]), thisrow) = 1
                    end;
                    
                for i:=0 to length(bk)-1 do
                begin
                    SetSysParam(PN_Stock(),bk[i]);
                    for j:= 1 to 30 do
                    begin                    
                        if istradeday(Tarr[t_index]-j) then
                        begin
                            BegTT:=Tarr[t_index]-j+18/24;
                            break;
                        end;
                    end;
                    EndTT := Tarr[t_index]+18/24;

                    data := select
                            %s
                        from %s
                        datekey BegTT to EndTT
                        of bk[i]
                        end;
                    FileName := bk[i];

                    rdo2 write_file(data, datetimetostr(Tarr[t_index])+'_'+FileName+'_%s'+'.csv');
                end;
            end;
            return 'Over';
        ''' % (begT, endT, bk_name, set_time_cycle_string, arg_string, table, type_string)
    else:
        code = '''
            begT := %s;
            endT := %s;
            bk   := '%s';

            Tarr := MarketTradeDayQk(begT, endT);        
            for t_index:=0 to length(Tarr)-1 do
            begin
                SetSysParam(PN_Stock(),bk);
                for i:= 1 to 30 do
                begin                    
                    if istradeday(Tarr[t_index]-i) then
                    begin
                        BegTT:=Tarr[t_index]-i+18/24;
                        break;
                    end;
                end;
                EndTT := Tarr[t_index]+18/24;

                %s
                data := select
                        %s
                    from %s
                    datekey BegTT to EndTT
                    of bk
                    end;
                FileName := bk;

                rdo2 write_file(data, datetimetostr(Tarr[t_index])+'_'+FileName+'_%s'+'.csv');
            end;
            return 'Over';
        ''' % (begT, endT, stock_name, set_time_cycle_string, arg_string, table, type_string)
    return code


def code_get_future_info(future_name, args):
    args_string = 'array('
    for arg in args:
        args_string += 'base(%s)' % args_dict[arg]
        if arg != args[-1]:
            args_string += ','
        else:
            args_string += ')'

    
    code = '''
            SetSysParam(Pn_Stock(), "%s");
            return %s;
        ''' % (future_name, args_string)
    return code


def get_future_info(c, bk_name, future_args):
    output_data = {arg:[] for arg in future_args}

    code = code_get_bk(bk_name)
    for future_name in c.exec(code).value():
        code = code_get_future_info(future_name, future_args)
        data = c.exec(code).value()
        for index, arg in enumerate(future_args):
            output_data[arg].append(data[index])
    df = pd.DataFrame.from_dict(output_data)
    df.to_csv(path['data_save']+'future_info_'+bk_name+'.csv', index=False)




if __name__ == "__main__":
    c = TS_INIT()

    code = code_get_data('20230710T', '20230710T', k_args,
                               stock_name='zn2401C21800',
                               time_cycle='cy_30m()')
    

    # code = code_OP_GetOptionChain('zn2401', '20231010T')
    # code = code_get_future_info('cu1311', future_args)
    r = c.exec(code)
    # print(r.value())
    # print(pd.DataFrame(r.value()))


    # get_future_info(c, '锌', future_args)
