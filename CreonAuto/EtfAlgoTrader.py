import os, sys, ctypes
import win32com.client
import requests
from datetime import datetime
import pandas as pd
import time, calendar

# CREON Plus 공통 Object
cpStatus = win32com.client.Dispatch('CpUtil.CpCybos')       # 시스템 상태 정보
cpTradeUtil = win32com.client.Dispatch('CpTrade.CpTdUtil')  # 주문 관련 도구
cpStock = win32com.client.Dispatch('DsCbo1.StockMst')       # 주식 종목별 검색
cpOhlc = win32com.client.Dispatch('CpSysDib.StockChart')    # OHLC 정보
cpBalance = win32com.client.Dispatch('CpTrade.CpTd6033')    # 계좌정보
cpCodeMgr = win32com.client.Dispatch('CpUtil.CpStockCode')  # 종목코드
cpCash = win32com.client.Dispatch('CpTrade.CpTdNew5331A')   # 주문 가능 금액
cpOrder = win32com.client.Dispatch('CpTrade.CpTd0311')

# CREON Plus 시스템 점검 함수
def check_creon_system():
    # 관리자 권한으로 프로세스 실행 여부
    if not ctypes.windll.shell32.IsUserAnAdmin():
        print('check_creon_system() : admin user -> FAILED')
        return False

    # 연결 여부 체크
    if (cpStatus.IsConnect == 0):
        print('check_creon_system() : connect to server -> FAILED')
        return False

    # 주문 관련 초기화
    if (cpTradeUtil.TradeInit(0) != 0):
        print('check_creon_system() : init trade -> FAILED')
        return False

    return True

def dbout(message) :
    nowDt = datetime.now().strftime('[%m%d %H:%M:%S]')
    print(nowDt, message)
    strbuf = nowDt + ' ' + message

    """ slack 메시지 설정 """
    webhook_url = "https://hooks.slack.com/services/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"

    payload = {"text": f"{strbuf}"}

    requests.post(webhook_url, json=payload)

def get_current_price(code) :
    cpStock.SetInputValue(0, code)
    cpStock.BlockRequest()

    item = {}
    item['cur_price'] = cpStock.GetHeaderValue(11)      #현재가
    item['ask'] = cpStock.GetHeaderValue(16)            #매수호가
    item['bid'] = cpStock.GetHeaderValue(17)            # 매도호가

    return item['cur_price'], item['ask'], item['bid']

def get_ohlc(code, qty):
    cpOhlc.SetInputValue(0, code)       # 종목코드
    cpOhlc.SetInputValue(1, ord('2'))   # 1: 기간, 2: 개수
    cpOhlc.SetInputValue(4, qty)        # 요청 개수
    cpOhlc.SetInputValue(5, [0, 2, 3, 4, 5])    # 0: 날짜, 2~5: OHLC
    cpOhlc.SetInputValue(6, ord('D'))   # D: 일단위
    cpOhlc.SetInputValue(9, ord('1'))   # 0: 무수정주가, 1: 수정주가
    cpOhlc.BlockRequest()

    count = cpOhlc.GetHeaderValue(3)    # 3: 수신 개수
    columns = ['open', 'high', 'low', 'close']
    index = []
    rows = []
    for i in range(count):
        index.append(cpOhlc.GetDataValue(0, i))
        rows.append([cpOhlc.GetDataValue(1, i), cpOhlc.GetDataValue(2, i),
                     cpOhlc.GetDataValue(3, i), cpOhlc.GetDataValue(4, i)])

    df = pd.DataFrame(rows, columns=columns, index=index)
    return df

def get_stock_balance(code):
    cpTradeUtil.TradeInit()
    acc = cpTradeUtil.AccountNumber[0]              # 계좌번호
    accFlag = cpTradeUtil.GoodsList(acc, 1)         # -1 : 전체, 1: 주식, 2: 선물/옵션
    cpBalance.SetInputValue(0, acc)                 # 계좌번호
    cpBalance.SetInputValue(1, accFlag[0])          # 상품 구분 - 주식 상품 중 첫 번째
    cpBalance.SetInputValue(2, 50)                  # 요청 건수 (최대 50)
    cpBalance.BlockRequest()

    if code == 'ALL':
        message = '계좌명: ' + str(cpBalance.GetHeaderValue(0)) + '\n'
        message += '결제잔고수량: ' + str(cpBalance.GetHeaderValue(1)) + '\n'
        message += '평가금액: ' + str(cpBalance.GetHeaderValue(3))  + '\n'
        message += '평가손익: ' + str(cpBalance.GetHeaderValue(4))  + '\n'
        message += '종목수: ' + str(cpBalance.GetHeaderValue(7))

        dbout(message)

    stocks = []
    for i in range(cpBalance.GetHeaderValue(7)):
        stock_code = cpBalance.GetDataValue(12, i)      # 종목코드
        stock_name = cpBalance.GetDataValue(10, i)      # 종목명
        stock_qty = cpBalance.GetDataValue(15, i)       # 수량
        if code == 'ALL':
            dbout(str(i+1) + ' ' + stock_code + '(' + stock_name + ')' + ':' + str(stock_qty))
            stocks.append({'code': stock_code, 'name': stock_name, 'qty': stock_qty})

        if stock_code == code:
            return stock_name, stock_qty

    if code == 'ALL':
        return stocks
    else:
        stock_name = cpCodeMgr.CodeToName(code)
        return stock_name, 0

def get_current_cash():
    cpTradeUtil.TradeInit()
    acc = cpTradeUtil.AccountNumber[0]
    accFlag = cpTradeUtil.GoodsList(acc, 1)
    cpCash.SetInputValue(0, acc)
    cpCash.SetInputValue(1, accFlag[0])
    cpCash.BlockRequest()

    return cpCash.GetHeaderValue(9)

def get_target_price(code):
    try:
        time_now = datetime.now()
        str_today = time_now.strftime('%Y%m%d')
        ohlc = get_ohlc(code, 10)
        if str_today == str(ohlc.iloc[0].name):
            today_open = ohlc.iloc[0].open
            lastday = ohlc.iloc[1]
        else:
            lastday = ohlc.iloc[0]
            today_open = lastday[3]
        lastday_high = lastday[1]
        lastday_low = lastday[2]
        target_price = today_open + (lastday_high - lastday_low) * 0.5
        return target_price
    except Exception as ex:
        dbout("'get_target_price() -> exception! " + str(ex) + "'")
        return None

def get_movingaverage(code, window):
    try:
        time_now = datetime.now()
        str_today = time_now.strftime('%Y%m%d')
        ohlc = get_ohlc(code, 40)
        if str_today == str(ohlc.iloc[0].name):
            lastday = ohlc.iloc[1].name
        else:
            lastday = ohlc.iloc[0].name
        closes = ohlc['close'].sort_index()
        ma = closes.rolling(window=window).mean()
        return ma.loc[lastday]
    except Exception as ex:
        dbout('get_movingaverage() -> exception!' + str(ex) + "'")
        return None

def buy_etf(code):
    try:
        global bought_list                          # 함수 내에서 값 변경할 수 있게 global로 지정
        if code in bought_list:                     # 매수 완료 종목이면 더 이상 안 사도록 함수 종료
            print('code:', code, 'in', bought_list)
            return False

        time_now = datetime.now()
        current_price, ask_price, bid_price = get_current_price(code)
        target_price = get_target_price(code)       # 매수 목표가
        ma5_price = get_movingaverage(code, 5)      # 5일 이동 평균가
        ma10_price = get_movingaverage(code, 10)    # 10일 이동 평균가
        buy_amount = get_current_cash()

        buy_qty = 0     # 매수할 수량 초기화
        if ask_price > 0: # 매수호가가 존재하면
            buy_qty = buy_amount // ask_price
        stock_name, stock_qty = get_stock_balance(code) # 종목명과 보유 수량 조회
        #print('bought_list:', bought_list, 'len(bought_list): ', len(bought_list), ' target_buy_count: ', target_buy_count)

        if current_price > target_price and current_price > ma5_price \
            and current_price > ma10_price:

            print(stock_name + '(' + str(code) + ') ' + str(buy_qty) + 'EA : ' + str(current_price) + ' meets the buy condition!')
            cpTradeUtil.TradeInit()
            acc = cpTradeUtil.AccountNumber[0]
            accFlag = cpTradeUtil.GoodsList(acc, 1)

            # 최유리 FOK 매수 주문
            cpOrder.SetInputValue(0, "2")           # 1: 매도, 2: 매수
            cpOrder.SetInputValue(1, acc)           # 계좌번호
            cpOrder.SetInputValue(2, accFlag[0])    # 상품구분 - 주식상품 중 첫번째
            cpOrder.SetInputValue(3, code)          # 종목코드
            cpOrder.SetInputValue(4, buy_qty)       # 매수할 수량
            cpOrder.SetInputValue(7, "2")           # 주문조건 0: 기본, 1: IOC, 2: FOK
            cpOrder.SetInputValue(8, "12")          # 5: 조건부, 12: 최유리, 13: 최우선

            ret = cpOrder.BlockRequest()
            print('최유리 FoK 매수 ->', stock_name, code, buy_qty, '->', ret)
            if ret == 4:
                remain_time = cpStatus.LimitRequestRemainTime
                print('주의: 연속 주문 제한에 걸림. 대기 시간:', remain_time/1000)
                time.sleep(remain_time/1000)
                return False
            time.sleep(2)
            print('현금주문 가능금액 :', buy_amount)
            stock_name, bought_qty = get_stock_balance(code)
            print('get_stock_balance :', stock_name, stock_qty)
            if bought_qty > 0:
                bought_list.append(code)
                dbout("'`'buy_etf(" + str(stock_name) + ' : ' + str(code) + ") -> " + str(bought_qty) + "EA bought!" + "'")

    except Exception as ex:
        dbout("'buy_etf(" + str(code) + ") -> exception! " + str(ex) + "'")

    def sell_all():
        """보유한 모든 종목을 최유리 지정가 IOC 조건으로 매도한다."""
        try:
            cpTradeUtil.TradeInit()
            acc = cpTradeUtil.AccountNumber[0]  # 계좌번호
            accFlag = cpTradeUtil.GoodsList(acc, 1)  # -1:전체, 1:주식, 2:선물/옵션
            while True:
                stocks = get_stock_balance('ALL')
                total_qty = 0
                for s in stocks:
                    total_qty += s['qty']
                if total_qty == 0:
                    return True
                for s in stocks:
                    if s['qty'] != 0:
                        cpOrder.SetInputValue(0, "1")  # 1:매도, 2:매수
                        cpOrder.SetInputValue(1, acc)  # 계좌번호
                        cpOrder.SetInputValue(2, accFlag[0])  # 주식상품 중 첫번째
                        cpOrder.SetInputValue(3, s['code'])  # 종목코드
                        cpOrder.SetInputValue(4, s['qty'])  # 매도수량
                        cpOrder.SetInputValue(7, "1")  # 조건 0:기본, 1:IOC, 2:FOK
                        cpOrder.SetInputValue(8, "12")  # 호가 12:최유리, 13:최우선
                        # 최유리 IOC 매도 주문 요청
                        ret = cpOrder.BlockRequest()
                        print('최유리 IOC 매도', s['code'], s['name'], s['qty'],
                                 '-> cpOrder.BlockRequest() -> returned', ret)
                        if ret == 4:
                            remain_time = cpStatus.LimitRequestRemainTime
                            print('주의: 연속 주문 제한, 대기시간:', remain_time / 1000)
                    time.sleep(1)
                time.sleep(30)
        except Exception as ex:
            dbout("sell_all() -> exception! " + str(ex))

    if __name__ == '__main__':
        try:
            symbol_list = ['A122630', 'A252670', 'A233740', 'A250780', 'A225130',
                           'A280940', 'A261220', 'A217770', 'A295000', 'A176950']
            bought_list = []  # 매수 완료된 종목 리스트
            target_buy_count = 5  # 매수할 종목 수
            buy_percent = 0.19
            print('check_creon_system() :', check_creon_system())  # 크레온 접속 점검
            stocks = get_stock_balance('ALL')  # 보유한 모든 종목 조회
            total_cash = int(get_current_cash())  # 100% 증거금 주문 가능 금액 조회
            buy_amount = total_cash * buy_percent  # 종목별 주문 금액 계산
            print('100% 증거금 주문 가능 금액 :', total_cash)
            print('종목별 주문 비율 :', buy_percent)
            print('종목별 주문 금액 :', buy_amount)
            print('시작 시간 :', datetime.now().strftime('%m/%d %H:%M:%S'))
            soldout = False;

            while True:
                t_now = datetime.now()
                t_9 = t_now.replace(hour=9, minute=0, second=0, microsecond=0)
                t_start = t_now.replace(hour=9, minute=5, second=0, microsecond=0)
                t_sell = t_now.replace(hour=15, minute=15, second=0, microsecond=0)
                t_exit = t_now.replace(hour=15, minute=20, second=0, microsecond=0)
                today = datetime.today().weekday()
                if today == 5 or today == 6:  # 토요일이나 일요일이면 자동 종료
                    print('Today is', 'Saturday.' if today == 5 else 'Sunday.')
                    sys.exit(0)
                if t_9 < t_now < t_start and soldout == False:
                    soldout = True
                    sell_all()
                if t_start < t_now < t_sell:  # AM 09:05 ~ PM 03:15 : 매수
                    for sym in symbol_list:
                        if len(bought_list) < target_buy_count:
                            buy_etf(sym)
                            time.sleep(1)
                    if t_now.minute == 30 and 0 <= t_now.second <= 5:
                        get_stock_balance('ALL')
                        time.sleep(5)
                if t_sell < t_now < t_exit:  # PM 03:15 ~ PM 03:20 : 일괄 매도
                    if sell_all() == True:
                        dbout('`sell_all() returned True -> self-destructed!`')
                        sys.exit(0)
                if t_exit < t_now:  # PM 03:20 ~ :프로그램 종료
                    dbout('`self-destructed!`')
                    sys.exit(0)
                time.sleep(3)
        except Exception as ex:
            dbout('`main -> exception! ' + str(ex) + '`')
