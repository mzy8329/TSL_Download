import pandas
import numpy as np

from pyTSL_code import * 


def readTxTand2TSLcode(path):
    with open(path, 'r') as file:
        return file.read()








code = '''
    t:=OP_GetOptionChain('SH510050',20210403T);
    return t[:,'StockID'];
'''



c = pyTSL.Client("hit", "20230427", "tsl.tinysoft.com.cn", 443)
c.login()




# def aasd(*args):
#     print(*args)
#     return 1
# c.set_callback(aasd)
# c.exec('''a := rdo mypycallback('ok'); echo a;'''); #rdo 会被调用，打印出 mypycallback "ok"


r = c.exec(code)
print(r.value())

c.logout()
















if __name__ == "__main__":




    pass