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
    'sectional yclose': "['sectional_yclose'] as 'prev_close'",
    'sectional_open': "['sectional_open'] as 'open'",
    'sectional_high': "['sectional_high'] as 'high'",
    'sectional_low': "['sectional_low'] as 'low'",
    'sectional_vol': "['sectional_vol'] as 'volume'",
    'sectional_amount': "['sectional_amount'] as 'amount'",
    'sectional_cjbs': "['sectional_cjbs'] as 'open_interest'"
}

tick_args = [
    'date', 'StockID', 'StockName', 'price', 'vol', 'amount', 'cjbs', 'bc1', 'buy1', 'sale1', 'sc1', 'sectional yclose',
    'sectional_open', 'sectional_high', 'sectional_low', 'sectional_vol', 'sectional_amount', 'sectional_cjbs'
]

k_args = [
    'date', 'StockID', 'StockName', 'price', 'vol', 'amount', 'cjbs', 'bc1', 'buy1', 'sale1', 'sc1', 'sectional yclose',
    'sectional_open', 'sectional_high', 'sectional_low', 'sectional_vol', 'sectional_amount', 'sectional_cjbs'
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


def code_get_data(begT, endT, args, bk_name = '', stock_name = '', time_cycle = ''):
    """
    获取并返回TSL语言代码, 执行该代码可直接下载某板块中的所有股票或某个股票的数据(k线或tick线)
    :param begT: 数据起始时间(不包括该日数据)
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
                    data := select
                            %s
                        from %s
                        datekey begT to endT
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
                %s
                data := select
                        %s
                    from %s
                    datekey begT to endT
                    of bk
                    end;
                FileName := bk;

                rdo2 write_file(data, datetimetostr(Tarr[t_index])+'_'+FileName+'_%s'+'.csv');
            end;
            return 'Over';
        ''' % (begT, endT, stock_name, set_time_cycle_string, arg_string, table, type_string)
    return code




if __name__ == "__main__":
    c = TS_INIT()

    # 上市期货 板块中AP2310
    # 30分钟, k线
    code = code_get_data('20230917T', '20230919T', k_args,
                               stock_name='AP2310',
                               time_cycle='cy_30m()')
    r = c.exec(code)

    # tick线
    code = code_get_data('20230917T', '20230919T', tick_args,
                               stock_name='AP2310')
    r = c.exec(code)


    #  获取 股票\指数成份\上证系列指数\上证策略指数\180分层 板块中SH603369
    # k线
    code = code_get_data('20230917T', '20230919T', k_args,
                            stock_name='SH603369',
                            time_cycle='cy_120m()')
    r = c.exec(code)

    # tick线
    code = code_get_data('20230917T', '20230919T', tick_args,
                            stock_name='SH603369')
    r = c.exec(code)


    # #  获取 股票\指数成份\上证系列指数\上证策略指数\180分层 板块
    # # k线
    # code = code_get_data('20230917T', '20230919T', k_args,
    #                         bk_name= '180分层',
    #                         #    stock_name='SH603369', 当bk_name不为空时，stock_name是否有值无意义   
    #                         time_cycle='cy_120m()')
    # r = c.exec(code)
    # # tick线
    # code = code_get_data('20230917T', '20230919T', tick_args,
    #                         bk_name= '180分层',
    #                         stock_name='SH603369')
    # r = c.exec(code)
