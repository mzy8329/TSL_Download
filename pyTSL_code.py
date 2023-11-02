import pyTSL
from datetime import datetime
import pandas as pd
import os

import time
import smtplib
from email.mime.text import MIMEText
from email.header import Header

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

# 模拟错误标记
I = 0

# 需要根据所需路径进行更改
path = {
    'data_save': 'C:/Code/Python/TSL_Download-master/TSL_Download-master/data/'
}


def sent_email(data):
    mail_host = "smtp.163.com"  # 设置服务器
    mail_user = "mzy8329@163.com"  # xxx是你的163邮箱用户名
    mail_pass = "UMYMKPXIWCPGISMQ"  # 口令是你设置的163授权密码
    
    sender = 'mzy8329@163.com'    #xxx是发送者邮箱
    receivers = ['253303476@qq.com']  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
    
    message = MIMEText(data)#邮箱内容
    message['From'] = Header("9_25_韩老师数据下载", 'utf-8')#发送者显示
    message['To'] = Header("mzy", 'utf-8')#接受者显示
    
    subject = '9_25_韩老师数据下载_异常次数过多'#邮箱标题
    message['Subject'] = Header(subject , 'utf-8')
    
    try:
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)  # 25 为 SMTP 端口号
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
        print
        "邮件发送成功"
    except smtplib.SMTPException:
        print
        "Error: 无法发送邮件"


def TS_INIT():
    c = pyTSL.Client("hit", "20230427", "tsl.tinysoft.com.cn", 443)
    c.login()
    return c

def write_file(df, dir, name):
    output_path = dir + name
    os.makedirs(dir, exist_ok=True)
    df= pd.DataFrame(df)
    df.to_csv(output_path, index=False)
    print(df.shape, output_path)
    return 1
pyTSL.register_proc('write_file', write_file)

def write_file_from_dict(data, dir, file_name):
    df = pd.DataFrame.from_dict(data)
    os.makedirs(dir, exist_ok=True)
    df.to_csv(dir+file_name, index=False)
    print(df.shape, dir + file_name)


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
    """
    输入期货名称和截止日期, 返回TSL语言代码, 可获得期权列表
    """
    code = '''
        PZ:= '%s';
        Endt:= %s;
        data:= OP_GetOptionChain(PZ, Endt);
        return data;
        ''' % (pz, Endt)
    return code

def code_get_data(begT, endT, args, down_dir, bk_name = '', stock_name = '', time_cycle = ''):
    """
    获取并返回TSL语言代码, 执行该代码可直接下载某板块中的所有期货或某个期货的数据(k线或tick线)
    :param begT: 数据起始时间(包括该日数据)
    :param endT: 数据终止时间(包括该日数据)
    :param args: 需要的数据参数
    :param down_dir: 下载路径
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

    down_dir += type_string + '/'

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
                    SetSysParam(pn_Stock(),bk[i]);
                    SetSysParam(pn_date(), Tarr[t_index]);
                    if not istradeday(Tarr[t_index]) then continue;
                    BegTT := ref(sp_time(),1)+18/24;
                    EndTT := Tarr[t_index]+18/24;

                    data := select
                            %s
                        from %s
                        datekey BegTT to EndTT
                        of bk[i]
                        end;
                    FileName := bk[i];

                    rdo2 write_file(data, '%s' +datetimetostr(Tarr[t_index])+'/', FileName+'_%s'+'.csv');
                end;
            end;
            return 'Over';
        ''' % (begT, endT, bk_name, set_time_cycle_string, arg_string, table, down_dir, type_string)
    else:
        code = '''
            begT := %s;
            endT := %s;
            bk   := '%s';
            SetSysParam(PN_Stock(),bk);
            Tarr := MarketTradeDayQk(begT, endT);                    
                                    
            for t_index:=0 to length(Tarr)-1 do
            begin
                SetSysParam(pn_date(), Tarr[t_index]);
                if not istradeday(Tarr[t_index]) then continue;
                BegTT := ref(sp_time(),1)+18/24;
                EndTT := Tarr[t_index]+18/24;
                %s
                data := select
                    %s
                    from %s
                    datekey BegTT to EndTT
                    of bk
                    end;

                FileName := bk;
                rdo2 write_file(data, '%s' + datetimetostr(Tarr[t_index])+'/', FileName+'_%s'+'.csv');
            end;
            return 'Over';
        ''' % (begT, endT, stock_name, set_time_cycle_string, arg_string, table, down_dir, type_string)
    return code

def code_get_future_baseInfo(future_name, args):
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


def code_get_day_data(T, stock_name, set_time_cycle_string, arg_string, table, down_dir, type):
    code = '''
            t := %s;
            bk := '%s';
            SetSysParam(PN_Stock(), bk);
            SetSysParam(pn_date(), t);

            BegTT := ref(sp_time(),1)+18/24;
            EndTT := t+18/24;
            %s
            data := select
                %s
                from %s
                datekey BegTT to EndTT
                of bk
                end;
            
            if istable(data) then rdo2 write_file(data, '%s' + datetimetostr(t)+'/', bk+'_%s'+'.csv');                        
            return 1;
        ''' % (T, stock_name, set_time_cycle_string, arg_string, table, down_dir, type)
    return code


def get_data(c, begT, endT, args_tick, args_k, down_dir, stock_name, time_cycle, T_start = ''):
    arg_string_k = ''
    for arg in args_k:
        arg_string_k += args_dict[arg]
        if arg != args_k[-1]:
            arg_string_k += ','
    set_time_cycle_string_k = 'setsysparam(pn_cycle(), %s);'% time_cycle
    table_k = 'markettable'

    arg_string_tick = ''
    for arg in args_tick:
        arg_string_tick += args_dict[arg]
        if arg != args_tick[-1]:
            arg_string_tick += ','  
    set_time_cycle_string_tick = ''
    table_tick = 'tradetable'


    Tarr = c.exec(code = '''
        SetSysParam(PN_Stock(), '%s');
        return MarketTradeDayQk(%s, %s);              
    ''' %(stock_name, begT, endT)).value()


    if T_start == '':
        is_T_start = True
    else:
        is_T_start = False
    for t_index in range(len(Tarr)-1, -1, -1):
        if T_start != '' and str(c.exec(code = 'return datetimetostr(%s);'% Tarr[t_index]).value()) == str(T_start):
            is_T_start = True
        daydata_isover = False
        error_time = 0
        while(not daydata_isover and is_T_start):
            if(c.exec(code = code_get_day_data(Tarr[t_index], stock_name, set_time_cycle_string_k, arg_string_k, table_k, down_dir+'k/', 'k')).value()
                and
                c.exec(code = code_get_day_data(Tarr[t_index], stock_name, set_time_cycle_string_tick, arg_string_tick, table_tick, down_dir+'tick/', 'tick')).value()):
                daydata_isover = True
            else:
                time.sleep(10)              
                error_time += 1
                if error_time > 10:
                    sent_email("stop_name:  " + stock_name + '\nstop_time:  ' + c.exec(code = 'return datetimetostr(%s);' % Tarr[t_index]).value())
                    return Tarr[t_index]

        # 模拟出现错误
        # global I
        # I += 1
        # if(I % 10 == 0):
        #     print(I)
        #     return Tarr[t_index]

    return True

def get_futureBK_baseInfo(c, bk_name, future_args):
    output_data = {arg:[] for arg in future_args}

    code = code_get_bk(bk_name)
    for future_name in c.exec(code).value():
        code = code_get_future_baseInfo(future_name, future_args)
        data = c.exec(code).value()
        for index, arg in enumerate(future_args):
            output_data[arg].append(data[index])
    return output_data
    
def get_bk_values(c, name):
    code = code_get_bk(name)
    return pd.DataFrame(c.exec(code).value())

def get_FuturesAllPZCode(c):
    code = '''
         return FuturesAllPZCode();
        '''
    return pd.DataFrame(c.exec(code).value())

def get_OP_GetUnderlyingSecurity(c):
    code = '''
         return OP_GetUnderlyingSecurity();
        '''
    return pd.DataFrame(c.exec(code).value())




def down_future_data(c, output_dir, begT, endT, time_cycle, future_start = '', future_name_start = '', T_start = ''):
    origin_dir = output_dir + '期货/'

    if os.path.exists(origin_dir + 'future_AllPZCode.csv'):
        future_AllPZCode = pd.read_csv(origin_dir + 'future_AllPZCode.csv')
    else:
        future_AllPZCode = get_FuturesAllPZCode(c)
        write_file_from_dict(future_AllPZCode, origin_dir, 'future_AllPZCode.csv')

    if os.path.exists(origin_dir + 'future_L_and_DL_StockNames.csv'):
        future_L_and_DL_StockNames = pd.read_csv(origin_dir + 'future_L_and_DL_StockNames.csv')
    else:
        future_L_and_DL_StockNames = get_bk_values(c, '上市期货;退市期货')
        write_file_from_dict(future_L_and_DL_StockNames, origin_dir, 'future_L_and_DL_StockNames.csv')

    if os.path.exists(origin_dir + '上市期货;退市期货_baseInfo.csv'):
        future_baseInfo = pd.read_csv(origin_dir + '上市期货;退市期货_baseInfo.csv')
    else:
        future_baseInfo = get_futureBK_baseInfo(c, '上市期货;退市期货', future_args)
        write_file_from_dict(future_baseInfo, origin_dir, '上市期货;退市期货_baseInfo.csv')


    if future_start == '':
        is_future_start = True
    else:
        is_future_start = False
    for future in future_AllPZCode.iloc[:, 0]:
        if future_start != '' and future == future_start:
            is_future_start = True

        if not is_future_start:
            continue

        future_dir = origin_dir + future + '/'

        if future_name_start == '':
            is_future_name_start = True
        else:
            is_future_name_start = False
        for future_name in future_L_and_DL_StockNames.iloc[:, 0]:
            if future_name_start != '' and future_name == future_name_start:
                is_future_name_start = True

            if not is_future_name_start:
                continue

            if future_name.upper()[0:len(future)] == future and future_name.upper()[len(future)+1].isnumeric():
                Ans = get_data(c = c,
                        begT = begT,
                        endT = endT, 
                        args_tick = tick_args,
                        args_k = k_args,
                        down_dir= future_dir,
                        stock_name = future_name,
                        time_cycle=time_cycle,
                        T_start = T_start)
                if(Ans != True):
                    return [future, future_name, Ans]

    print("download future over")
    return True


def down_option_data(c, output_dir, data_begT, data_endT, time_cycle, ddl_begT, ddl_endT, op_start = '', ddl_time_start = '', op_name_start = '', T_start = ''):
    origin_dir = output_dir + '期权/'

    if os.path.exists(origin_dir + 'OP_UnderlyingSecurity.csv'):
        OP_UnderlyingSecurity = pd.read_csv(origin_dir + 'OP_UnderlyingSecurity.csv')
    else:
        OP_UnderlyingSecurity = get_OP_GetUnderlyingSecurity(c)
        write_file_from_dict(OP_UnderlyingSecurity, origin_dir, 'OP_UnderlyingSecurity.csv')


    if op_start == '':
        is_op_start = True
    else:
        is_op_start = False
    for term in OP_UnderlyingSecurity.iloc[:, 0]:
        if op_start != '' and term == op_start:
            is_op_start = True

        if not is_op_start:
            continue


        op_dir = origin_dir + term + '/'
        code = '''
            return MarketTradeDayQk(%s, %s);
        ''' %(ddl_begT, ddl_endT)
        ddl_Tarr = c.exec(code = code).value()

        if ddl_time_start == '':
            is_ddl_time_start = True
        else:
            is_ddl_time_start = False
        for ddl_t_index in range(len(ddl_Tarr)-1, -1, -1):
            ddl_t = ddl_Tarr[ddl_t_index]

            if ddl_time_start != '' and str(ddl_t) == str(ddl_time_start):
                is_ddl_time_start = True

            if not is_ddl_time_start:
                continue
            
            op_ddl_dir = op_dir + '截止日_' + str(c.exec(code='return datetimetostr(%s);'%ddl_t).value()) + '/'
            opChain_data = pd.DataFrame(c.exec(code_OP_GetOptionChain(term, ddl_t)).value())
            write_file_from_dict(opChain_data, op_ddl_dir, 'OptionChain.csv')


            if op_name_start == '':
                is_op_name_start = True
            else:
                is_op_name_start = False
            for op_name in opChain_data.iloc[:, 0]:
                if op_name_start != '' and op_name == op_name_start:
                    is_op_name_start = True

                if not is_op_name_start:
                    continue

                Ans = get_data(c = c,
                        begT = data_begT,
                        endT = data_endT, 
                        args_tick = tick_args,
                        args_k = k_args,
                        down_dir= op_ddl_dir,
                        stock_name = op_name,
                        time_cycle=time_cycle,
                        T_start = T_start)
                if(Ans != True):
                    return [term, ddl_t, op_name, Ans]

    print("download future over")
    return True




if __name__ == "__main__":
    c = TS_INIT()
    time_start = datetime.today()


    future_ans = down_future_data(c, path['data_save'], begT = '20230510T', endT = '20230510T', time_cycle='cy_30m()')
    while(future_ans != True):
        print('future download error, stop at ' + str(future_ans) + ', retry now')
        sent_email('future download error, stop at ' + str(future_ans) + ', retry now')
        time.sleep(5)
        future_ans = down_future_data(c, path['data_save'], begT = '20230510T', endT = '20230510T', time_cycle='cy_30m()', future_start=future_ans[0], future_name_start=future_ans[1], T_start=future_ans[2])
    time_future_used = datetime.today()


    option_ans =  down_option_data(c, path['data_save'], data_begT='20230510T', data_endT='20230510T', time_cycle='cy_30m()', ddl_begT='20231009T', ddl_endT='20231009T',)
    while(option_ans != True):
        print('future download error, stop at ' + str(option_ans) + ', retry now')
        sent_email('option download error, stop at ' + str(option_ans) + ', retry now')
        time.sleep(5)
        option_ans =  down_option_data(c, path['data_save'], data_begT='20230510T', data_endT='20230510T', time_cycle='cy_30m()', ddl_begT='20231010T', ddl_endT='20231010T',
                                       op_start=option_ans[0], ddl_time_start=option_ans[1], op_name_start=option_ans[2], T_start=option_ans[3])
 
    time_option_used = datetime.today()


    print('time_start: ' + str(time_start))
    print('time_future_used: ' + str(time_future_used))
    print('time_option_used: ' + str(time_option_used))