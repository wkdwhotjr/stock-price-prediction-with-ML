##### 사용자 함수 : Index DataFrame 생성 #####
def get_index(market) :
    import pandas as pd
    from datetime import datetime
    from pykrx import stock

    if market == 'KOSPI' :
        kospi_index_df = pd.DataFrame()
        kospi_in_list=[]
        for ticker in stock.get_index_ticker_list() :
            kospi_in_list.append(stock.get_index_ticker_name(ticker))
        kospi_index_df['INDEX'] = kospi_in_list
        kospi_index_df['CODE'] = stock.get_index_ticker_list()
        kospi_index_df.set_index('CODE', drop=True, inplace=True)
        return kospi_index_df
    
    if market == 'KOSDAQ' :
        kosdaq_index_df = pd.DataFrame()
        kosdaq_in_list=[]
        for ticker in stock.get_index_ticker_list(market='KOSDAQ') :
            kosdaq_in_list.append(stock.get_index_ticker_name(ticker))
        kosdaq_index_df['INDEX'] = kosdaq_in_list
        kosdaq_index_df['CODE'] = stock.get_index_ticker_list(market='KOSDAQ')
        kosdaq_index_df.set_index('CODE', drop=True, inplace=True)
        return kosdaq_index_df


##### 사용자 함수 : 해당 Index에 속한 종목 DataFrame 조회 #####
def get_itm_in_idx(idx_code) :
    import pandas as pd
    from pykrx import stock
    c_list = stock.get_index_portfolio_deposit_file(idx_code)
    s_list = []
    for c in c_list :
        value = stock.get_market_ticker_name(c)
        s_list.append(value)
    rdf = pd.DataFrame()
    rdf['CODE'] = c_list
    rdf['NAME'] = s_list
    rdf.set_index('CODE', inplace=True)
    return rdf


##### 사용자 함수 : WICS sector 크롤링 #####
def get_WICS(ticker) :
    
    import requests
    from bs4 import BeautifulSoup

    url = 'https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={}'.format(ticker)

    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    value = str(soup.select('.line-left')[9])
    result = value[29:-5]
    return result


##### 사용자 함수 : ticker : sector dictionary 생성 #####
# 그냥 함수에 포함시키니깐 [WinError : 10060] 계속해서 발생. 따라서 부득이하게 분리
def get_ticker_sector_dict() :

    import pandas as pd
    
    idx_code_list = ['1001', '2001']

    ticker_sector_dict = {}

    for idx in idx_code_list :
        if idx == '1001' :
            tickers = get_itm_in_idx('1001').index
            for ticker in tickers :
                sector = get_WICS(ticker)
                ticker_sector_dict[ticker] = sector
        elif idx == '2001' :
            tickers = get_itm_in_idx('2001').index
            for ticker in tickers :
                sector = get_WICS(ticker)
                ticker_sector_dict[ticker] = sector
                
    return ticker_sector_dict


##### 사용자 함수 : NAME : MARKET - TICKER - SECTOR Dictionary Update #####
def dictionary_update() :
    
    import pandas as pd
    import pickle
            
    # Dictionary 생성
    name_code_sector_dict = {}
    
    # ticker_sector_dict 크롤링
    ticker_sector_dict = get_ticker_sector_dict()

    # Dictionary에 값들 맵핑 
    idx_code_list = ['1001', '2001']

    for idx_code in idx_code_list :
        if idx_code == '1001' :
            idx_name_dict = dict(zip(get_itm_in_idx('1001').index, get_itm_in_idx('1001').NAME))
            for ticker in idx_name_dict.keys() :
                name_code_sector_dict[idx_name_dict[ticker]] = ['KOSPI', ticker, ticker_sector_dict[ticker]]
        elif idx_code == '2001' :
            idx_name_dict = dict(zip(get_itm_in_idx('2001').index, get_itm_in_idx('2001').NAME))
            for ticker in idx_name_dict.keys() :
                name_code_sector_dict[idx_name_dict[ticker]] = ['KOSDAQ', ticker, ticker_sector_dict[ticker]]
    
    # pickle 파일로 저장
    with open('./dataset/N_MTS_dict.pkl', 'wb') as f:
        pickle.dump(name_code_sector_dict, f)
    print('NAME : MARKET - TICKER - SECTOR Dictionary Update Complete')


##### 사용자 함수 : 기본 주가정보 DataFrame 생성 (Index 기준) #####
def get_stock_price_data(start_date, end_date, stock_list) : # *stock_names

    import pandas as pd
    import pickle
    from datetime import datetime, timedelta, date
    from dateutil.relativedelta import relativedelta
    from pykrx import stock
    import numpy as np

    # NAME : MARKET - TICKER - SECTOR Dictionary 로드
    with open('./dataset/N_MTS_dict.pkl', 'rb') as f :
        name_code_sector_dict = pickle.load(f)  
    
    # 기본 OHLCV DataFrame 생성
    # stock_list = list(stock_names)
    df = stock.get_market_ohlcv_by_date(start_date, end_date, name_code_sector_dict[stock_list[0]][1])
    df['NAME'] = stock_list[0]
    df['SECTOR'] = name_code_sector_dict[stock_list[0]][2]
    
    for stock_name in stock_list[1:] :
        tmp_df = stock.get_market_ohlcv_by_date(start_date, end_date, name_code_sector_dict[stock_name][1])
        tmp_df['NAME'] = stock_name
        tmp_df['SECTOR'] = name_code_sector_dict[stock_name][2]
        df = df.append(tmp_df)
        print(stock_name, 'OHLCV complete')
    
    df.reset_index(inplace=True)
    rename_dict = {'날짜' : 'DATE', '시가' : 'OPEN', '고가' : 'HIGH', '저가' : 'LOW',
                   '종가' : 'CLOSE', '거래량' : 'VOLUME'}
    df.rename(rename_dict, axis=1, inplace=True)
    custom_columns = ['DATE', 'NAME', 'SECTOR', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
    df = df[custom_columns]
    
    
    # 종가의 최근 1달간 표준편차 산출 후 1M_CLOSE_STD로 삽입
    # def m_var(x) :
    #     target_date = x['DATE'] - relativedelta(months=1)
    #     target_name = x['NAME']
        
    #     mask = df.NAME == target_name
    #     masked_df = df[mask]
        
    #     if target_date in masked_df.DATE.unique() :
    #         target_date_idx = masked_df[masked_df.DATE == target_date].index[0]
    #         x_date_idx = masked_df[masked_df.DATE == x['DATE']].index[0]
    #         close_series = masked_df.loc[target_date_idx : x_date_idx, 'CLOSE']
    #         return np.std(close_series)
        
    #     else :
    #         if target_date < date(2017, 10, 10) :
    #             return 0
    #         elif target_date >= date(2017, 10, 10) :
    #             for n in range(1, 6) :
    #                 business_day = target_date + timedelta(n)
    #                 if len(masked_df[masked_df.DATE == business_day]) != 0 :
    #                     break
    #         target_date_idx = masked_df[masked_df.DATE == business_day].index[0]
    #         x_date_idx = masked_df[masked_df.DATE == x['DATE']].index[0]
    #         close_series = masked_df.loc[target_date_idx : x_date_idx, 'CLOSE']
    #         return np.std(close_series)
    
    # df['1M_CLOSE_STD'] = df.apply(m_var, axis=1)
    
                  
    # PER / PBR / EPS / BPS / DIV / DPS 삽입
    if len(stock_list) == 1 :
      pdf = stock.get_market_fundamental_by_date(start_date, end_date, name_code_sector_dict[stock_list[0]][1])
      pdf['NAME'] = stock_list[0]
    elif len(stock_list) >= 2 :
      pdf = stock.get_market_fundamental_by_date(start_date, end_date, name_code_sector_dict[stock_list[0]][1])
      pdf['NAME'] = stock_list[0]
      for stock_name in stock_list[1:] :
        tmp_df_2 = stock.get_market_fundamental_by_date(start_date, end_date, name_code_sector_dict[stock_name][1])
        tmp_df_2['NAME'] = stock_name
        pdf = pdf.append(tmp_df_2)
        print(stock_name, 'PER data complete')
    pdf.reset_index(inplace=True)
    if pdf.columns[0] == '날짜' :
        pdf.rename({'날짜' : 'DATE'}, axis=1, inplace=True)
    elif pdf.columns[0] == 'index' :
        pdf.rename({'index' : 'DATE'}, axis=1, inplace=True)

              
    def per(x) :
        mask = (pdf.DATE == x['DATE']) & (pdf.NAME == x['NAME'])
        if len(pdf[mask]['PER']) != 0 :
            return pdf[mask]['PER'].values[0]
        else : return 0
    def pbr(x) :
        mask = (pdf.DATE == x['DATE']) & (pdf.NAME == x['NAME'])
        if len(pdf[mask]['PBR']) != 0 :
            return pdf[mask]['PBR'].values[0]
        else : return 0    
    def eps(x) :
        mask = (pdf.DATE == x['DATE']) & (pdf.NAME == x['NAME'])
        if len(pdf[mask]['EPS']) != 0 :
            return pdf[mask]['EPS'].values[0]
        else : return 0
    def bps(x) :
        mask = (pdf.DATE == x['DATE']) & (pdf.NAME == x['NAME'])
        if len(pdf[mask]['BPS']) != 0 :
            return pdf[mask]['BPS'].values[0]
        else : return 0
    def div(x) :
        mask = (pdf.DATE == x['DATE']) & (pdf.NAME == x['NAME'])
        if len(pdf[mask]['DIV']) != 0 :
            return pdf[mask]['DIV'].values[0]
        else : return 0
    def dps(x) :
        mask = (pdf.DATE == x['DATE']) & (pdf.NAME == x['NAME'])
        if len(pdf[mask]['DPS']) != 0 :
            return pdf[mask]['DPS'].values[0]
        else : return 0
       
    df['PER'] = df.apply(per, axis=1)
    print('PER Mapping complete')
    df['PBR'] = df.apply(pbr, axis=1)
    print('PBR Mapping complete')
    df['EPS'] = df.apply(eps, axis=1)
    print('EPS Mapping complete')
    df['BPS'] = df.apply(bps, axis=1)
    print('BPS Mapping complete')
    df['DIV'] = df.apply(div, axis=1)
    print('DIV Mapping complete')
    df['DPS'] = df.apply(dps, axis=1)
    print('DPS Mapping complete')
    
    # 산업 평균 PER, PBR (SECTOR), 시장 평균 PER, PBR (MARKET) 추가
    # def per_sec(x) :
    #   mask = (df.DATE == x['DATE']) & (df.SECTOR == x['SECTOR'])
    #   return df[mask]['PER'].mean()
    # def pbr_sec(x) :
    #   mask = (df.DATE == x['DATE']) & (df.SECTOR == x['SECTOR'])
    #   return df[mask]['PBR'].mean()
    # def per_mar(x) :
    #   mask = df.DATE == x['DATE']
    #   return df[mask]['PER'].mean()
    # def pbr_mar(x) :
    #   mask = df.DATE == x['DATE']
    #   return df[mask]['PBR'].mean()
    
    # df['PER(SECTOR)'] = df.apply(per_sec, axis=1)
    # print('PER(SECTOR) Mapping complete')
    # df['PBR(SECTOR)'] = df.apply(pbr_sec, axis=1)
    # print('PBR(SECTOR) Mapping complete')
    # df['PER(MARKET)'] = df.apply(per_mar, axis=1)
    # print('PER(MARKET) Mapping complete')
    # df['PBR(MARKET)'] = df.apply(pbr_mar, axis=1)
    # print('PBR(MARKET) Mapping complete')
    
    # 시가총액 CAPITALIZATION, 상장주식수 NUMB_OF_SHARES 추가
    # stock_list : parameter로 들어온 종목들 이름
    # 종목코드 리스트 생성
    # tickers = []
    # for name in stock_list :
    #   ticker = name_code_sector_dict[name][1]
    #   tickers.append(ticker)
    
    if len(stock_list) == 1 :
      cap_df = stock.get_market_cap_by_date(start_date, end_date, name_code_sector_dict[stock_list[0]][1])
      cap_df['NAME'] = stock_list[0]
    elif len(stock_list) >= 2 :
      cap_df = stock.get_market_cap_by_date(start_date, end_date, name_code_sector_dict[stock_list[0]][1])
      cap_df['NAME'] = stock_list[0]
      for stock_name in stock_list[1:] :
        tmp_df = stock.get_market_cap_by_date(start_date, end_date, name_code_sector_dict[stock_name][1])
        tmp_df['NAME'] = stock_name
        cap_df = cap_df.append(tmp_df)
        print(stock_name, 'CAPITAL data complete')

    cap_df.reset_index(inplace=True)
    if cap_df.columns[0] == '날짜' :
        cap_df.rename({'날짜' : 'DATE', '시가총액' : 'CAPITALIZATION', '상장주식수' : 'NUMB_OF_SHARES'}, axis=1, inplace=True)
    elif cap_df.columns[0] == 'index' :
        cap_df.rename({'index' : 'DATE', '시가총액' : 'CAPITALIZATION', '상장주식수' : 'NUMB_OF_SHARES'}, axis=1, inplace=True)

    def capitalization(x) :
      mask = (cap_df.DATE == x['DATE']) & (cap_df.NAME == x['NAME'])
      return cap_df[mask]['CAPITALIZATION'].values[0]
    def numb_of_shares(x) :
      mask = (cap_df.DATE == x['DATE']) & (cap_df.NAME == x['NAME'])
      return cap_df[mask]['NUMB_OF_SHARES'].values[0]
    
    df['CAPITALIZATION'] = df.apply(capitalization, axis=1)
    print('CAPITALIZATION Mapping complete')
    df['NUMB_OF_SHARES'] = df.apply(numb_of_shares, axis=1)
    print('NUMB_OF_SHARES Mapping complete')
    
    # 기관합계 INSTITUTION(NP), 기타법인 CORP(NP), 개인 INDIVIDUAL(NP), 외국인 FOREIGN(NP) 추가
    # NP : 순매수거래대금
    if len(stock_list) == 1 :
      tv_df = stock.get_market_trading_value_by_date(start_date, end_date, name_code_sector_dict[stock_list[0]][1])
      tv_df['NAME'] = stock_list[0]
    elif len(stock_list) >= 2 :
      tv_df = stock.get_market_trading_value_by_date(start_date, end_date, name_code_sector_dict[stock_list[0]][1])
      tv_df['NAME'] = stock_list[0]
      for stock_name in stock_list[1:] :
        tmp_df = stock.get_market_trading_value_by_date(start_date, end_date, name_code_sector_dict[stock_name][1])
        tmp_df['NAME'] = stock_name
        tv_df = tv_df.append(tmp_df)
        print(stock_name, 'NP data complete')
    
    tv_df.reset_index(inplace=True)
    if tv_df.columns[0] == '날짜' :
        tv_df.rename({'날짜' : 'DATE', '기관합계' : 'INSTITUTION(NP)', '기타법인' : 'CORP(NP)',\
                  '개인' : 'INDIVIDUAL(NP)', '외국인합계' : 'FOREIGN(NP)'}, axis=1, inplace=True)
    elif tv_df.columns[0] == 'index' :
        tv_df.rename({'index' : 'DATE', '기관합계' : 'INSTITUTION(NP)', '기타법인' : 'CORP(NP)',\
                  '개인' : 'INDIVIDUAL(NP)', '외국인합계' : 'FOREIGN(NP)'}, axis=1, inplace=True)
        
    def institution_np(x) :
      mask = (tv_df.DATE == x['DATE']) & (tv_df.NAME == x['NAME'])
      if len(tv_df[mask]['INSTITUTION(NP)']) == 0 :
        return 0
      elif len(tv_df[mask]['INSTITUTION(NP)']) != 0 :
        return tv_df[mask]['INSTITUTION(NP)'].values[0]
    def corp_np(x) :
      mask = (tv_df.DATE == x['DATE']) & (tv_df.NAME == x['NAME'])
      if len(tv_df[mask]['CORP(NP)']) == 0 :
        return 0
      elif len(tv_df[mask]['CORP(NP)']) != 0 :
        return tv_df[mask]['CORP(NP)'].values[0]
    def individual_np(x) :
      mask = (tv_df.DATE == x['DATE']) & (tv_df.NAME == x['NAME'])
      if len(tv_df[mask]['INDIVIDUAL(NP)']) == 0 :
        return 0
      elif len(tv_df[mask]['INDIVIDUAL(NP)']) != 0 :
        return tv_df[mask]['INDIVIDUAL(NP)'].values[0]
    def foreign_np(x) :
      mask = (tv_df.DATE == x['DATE']) & (tv_df.NAME == x['NAME'])
      if len(tv_df[mask]['FOREIGN(NP)']) == 0 :
        return 0
      elif len(tv_df[mask]['FOREIGN(NP)']) != 0 :
        return tv_df[mask]['FOREIGN(NP)'].values[0]
        
    df['INSTITUTION(NP)'] = df.apply(institution_np, axis=1)
    print('INSTITUTION(NP) Mapping complete')
    df['CORP(NP)'] = df.apply(corp_np, axis=1)
    print('CORP(NP) Mapping complete')
    df['INDIVIDUAL(NP)'] = df.apply(individual_np, axis=1)
    print('INDIVIDUAL(NP) Mapping complete')
    df['FOREIGN(NP)'] = df.apply(foreign_np, axis=1)
    print('FOREIGN(NP) Mapping complete')
    
    return df



##### 재무비율 생성 class #####
class FinancialStatements :

    def __init__(self, path=None) :
        self.path = path
    
    ##### 기간 list #####
    qt_list = ['20201231', '20200930', '20200630', '20200331', 
               '20191231', '20190930', '20190630', '20190331', 
               '20181231', '20180930', '20180630', '20180331', 
               '20171231', '20170930']
    
    ##### 산출 항목 list #####
    itms_list = ['총자산', '전기총자산', '총자본', '총부채', '유동자산', '유동부채', 
                 '재고자산', '비유동자산', '매출채권', '매출액', '전기매출액', '당기순이익',
                 '전기당기순이익', '영업이익', '총자산증가율', '매출액증가율', '당기순이익증가율',
                 '자기자본증가율', '매출액영업이익률', '매출액순이익률', '총자본순이익률', 
                 '경영자산영업이익률', '유동비율', '당좌비율', '고정비율', '부채비율', 
                 '총자본회전율', '매출채권회전율', '재고자산회전율', 'ROE']
    
    ##### 기간 dictionary #####
    column_dict = {}
    column_dict['20201231'] = ['20200930', '20200101-20201231', '20200101-20200930', '20200701-20200930', 'a']
    column_dict['20200930'] = ['20200630', '20200701-20200930', '20200401-20200630']
    column_dict['20200630'] = ['20200331', '20200401-20200630', '20200101-20200331']
    column_dict['20200331'] = ['20191231', '20200101-20200331', '20190101-20191231', '20190101-20190930']
    column_dict['20191231'] = ['20190930', '20190101-20191231', '20190101-20190930', '20190701-20190930', 'a']
    column_dict['20190930'] = ['20190630', '20190701-20190930', '20190401-20190630']
    column_dict['20190630'] = ['20190331', '20190401-20190630', '20190101-20190331']
    column_dict['20190331'] = ['20181231', '20190101-20190331', '20180101-20181231', '20180101-20180930']
    column_dict['20181231'] = ['20180930', '20180101-20181231', '20180101-20180930', '20180701-20180930', 'a']
    column_dict['20180930'] = ['20180630', '20180701-20180930', '20180401-20180630']
    column_dict['20180630'] = ['20180331', '20180401-20180630', '20180101-20180331']
    column_dict['20180331'] = ['20171231', '20180101-20180331', '20170101-20171231', '20170101-20170930']
    column_dict['20171231'] = ['20170930', '20170101-20171231', '20170101-20170930', '20170701-20170930', 'a']
    column_dict['20170930'] = ['20170630', '20170701-20170930', '20170401-20170630']

    ##### 재무제표 클렌징 사용자 함수 #####
    def cleansing(self, df) :
        
        import pandas as pd
        
        for i in range(len(df.columns)) :
            
            if df.iloc[0,:][i] == 'label_ko' :
                column_no = i
            if (df.columns[i] == '20201231') or (df.columns[i] == '20200930') or (df.columns[i] == '20200701-20200930') or\
                (df.columns[i] == '20200101-20201231') or (df.columns[i] == '20201001-20201231') or (df.columns[i] == '20200601-20200831') :
                    data_no = i
                    break
        
        index = df[df.columns[column_no]][2:]
        result_df = pd.DataFrame(index=index)
        for k in range(data_no, len(df.columns)) :
            column_name = df.columns[k]
            result_df[column_name] = df[column_name][2:].tolist()
        
        return result_df

    
    ##### 재무비율 산출 #####
    # df = 재무비율칼럼을 추가할 DataFrame
    def get_fsr(self) :

        import numpy as np
        import pandas as pd
        import os
        from pykrx import stock
        
        # path에 있는 재무제표 excel 파일들 이름 list 작성
        fs_list = os.listdir(self.path)

        # 산출한 재무비율 pickle을 저장할 폴더 생성
        try:
            if not os.path.exists('./dataset/fsr'):
                os.makedirs('./dataset/fsr')
        except OSError:
            print ('Error: {} was created'.format('./dataset/fsr'))

        for fs_file in fs_list :
            
            fs_code = fs_file[:-5]
            fs_name = stock.get_market_ticker_name(fs_code)

            # 결과 담을 DataFrame 생성
            fsr_df = pd.DataFrame(columns=self.qt_list, index=self.itms_list)

            # 해당기업 재무상태표 load
            bs_df = pd.read_excel('{}/{}'.format(self.path, fs_file), sheet_name=0, engine='openpyxl')
            bs_df = self.cleansing(bs_df)
            bs_df.to_pickle('./dataset/bs_df/{}_bs_df.pkl'.format(fs_code))
            
            # 해당기업 포괄손익계산서
            cis_df = pd.read_excel('{}/{}'.format(self.path, fs_file), sheet_name=2, engine='openpyxl')
            cis_df = self.cleansing(cis_df)
            cis_df.to_pickle('./dataset/cis_df/{}_cis_df.pkl'.format(fs_code))
        
            # 각 기간별로 for문을 사용하여 재무비율 산출
            for qt in self.qt_list :

                if qt in bs_df.columns :

                    ## 재무상태표 ##
                    # 총자산
                    if '자산총계' in bs_df.index :
                        asst = bs_df[qt]['자산총계']
                    elif '자산 총계' in bs_df.index :
                        asst = bs_df[qt]['자산 총계']
                    # 전기 총자산
                    if '자산총계' in bs_df.index :
                        b_asst = bs_df[self.column_dict[qt][0]]['자산총계']
                    elif '자산 총계' in bs_df.index :
                        b_asst = bs_df[self.column_dict[qt][0]]['자산 총계']
                    # 총자본
                    if '자본총계' in bs_df.index :
                        eq = bs_df[qt]['자본총계']
                    elif '자본 총계' in bs_df.index :
                        eq = bs_df[qt]['자본 총계']
                    # 총부채
                    if '부채총계' in bs_df.index :
                        lia = bs_df[qt]['부채총계']
                    elif '부채 총계' in bs_df.index :
                        lia = bs_df[qt]['부채 총계']
                    # 유동자산
                    if '유동자산' in bs_df.index :
                        r_asset = bs_df[qt]['유동자산']
                    elif 'I.유동자산' in bs_df.index :
                        r_asset = bs_df[qt]['I.유동자산']
                    # 유동부채
                    if '유동부채' in bs_df.index :
                        r_debt = bs_df[qt]['유동부채']
                    elif 'I.유동부채' in bs_df.index :
                        r_debt = bs_df[qt]['I.유동부채']
                    # 재고자산
                    if '재고자산' in bs_df.index :
                        st_asset = bs_df[qt]['재고자산']
                    elif '13.재고자산' in bs_df.index :
                        st_asset = bs_df[qt]['13.재고자산']
                    # 비유동자산
                    if '비유동자산' in bs_df.index :
                        nr_asset = bs_df[qt]['비유동자산']
                    elif 'Ⅱ.비유동자산' in bs_df.index :
                        nr_asset = bs_df[qt]['Ⅱ.비유동자산']
                    # 매출채권
                    if '매출채권및기타채권' in bs_df.index :
                        if str(type(bs_df[qt]['매출채권및기타채권'])) == "<class 'pandas.core.series.Series'>" :
                            sa_c = bs_df[qt]['매출채권및기타채권'][0]
                        else : sa_c = bs_df[qt]['매출채권및기타채권']
                    elif '매출채권' in bs_df.index :
                        sa_c = bs_df[qt]['매출채권']
                    elif '매출채권 및 기타채권' in bs_df.index :
                        sa_c = bs_df[qt]['매출채권 및 기타채권']
                    
                
                    ## 포괄손익계산서 ##
                    # 매출액
                    if '매출액' in cis_df.index :
                        if len(self.column_dict[qt]) == 5 :
                            rev = cis_df[self.column_dict[qt][1]]['매출액'] - cis_df[self.column_dict[qt][2]]['매출액']
                        elif len(self.column_dict[qt]) == 4 :
                            rev = cis_df[self.column_dict[qt][1]]['매출액']
                        elif len(self.column_dict[qt]) == 3 :
                            rev = cis_df[self.column_dict[qt][1]]['매출액']
                    elif '매출' in cis_df.index :
                        if len(self.column_dict[qt]) == 5 :
                            rev = cis_df[self.column_dict[qt][1]]['매출'] - cis_df[self.column_dict[qt][2]]['매출']
                        elif len(self.column_dict[qt]) == 4 :
                            rev = cis_df[self.column_dict[qt][1]]['매출']
                        elif len(self.column_dict[qt]) == 3 :
                            rev = cis_df[self.column_dict[qt][1]]['매출']
                    elif '수익(매출액)' in cis_df.index :
                        if len(self.column_dict[qt]) == 5 :
                            rev = cis_df[self.column_dict[qt][1]]['수익(매출액)'] - cis_df[self.column_dict[qt][2]]['수익(매출액)']
                        elif len(self.column_dict[qt]) == 4 :
                            rev = cis_df[self.column_dict[qt][1]]['수익(매출액)']
                        elif len(self.column_dict[qt]) == 3 :
                            rev = cis_df[self.column_dict[qt][1]]['수익(매출액)']
                    # 전기 매출액
                    if '매출액' in cis_df.index :
                        if len(self.column_dict[qt]) == 5 :
                            b_rev = cis_df[self.column_dict[qt][3]]['매출액']
                        elif len(self.column_dict[qt]) == 4 :
                            b_rev = cis_df[self.column_dict[qt][2]]['매출액'] - cis_df[self.column_dict[qt][3]]['매출액']
                        elif len(self.column_dict[qt]) == 3 :
                            b_rev = cis_df[self.column_dict[qt][2]]['매출액']
                    elif '매출' in cis_df.index :
                        if len(self.column_dict[qt]) == 5 :
                            b_rev = cis_df[self.column_dict[qt][3]]['매출']
                        elif len(self.column_dict[qt]) == 4 :
                            b_rev = cis_df[self.column_dict[qt][2]]['매출'] - cis_df[self.column_dict[qt][3]]['매출']
                        elif len(self.column_dict[qt]) == 3 :
                            b_rev = cis_df[self.column_dict[qt][2]]['매출']
                    elif '수익(매출액)' in cis_df.index :
                        if len(self.column_dict[qt]) == 5 :
                            b_rev = cis_df[self.column_dict[qt][3]]['수익(매출액)']
                        elif len(self.column_dict[qt]) == 4 :
                            b_rev = cis_df[self.column_dict[qt][2]]['수익(매출액)'] - cis_df[self.column_dict[qt][3]]['수익(매출액)']
                        elif len(self.column_dict[qt]) == 3 :
                            b_rev = cis_df[self.column_dict[qt][2]]['수익(매출액)']        
                            
                    # 당기순이익
                    if '당기순이익' in cis_df.index :
                        if len(self.column_dict[qt]) == 5 :
                            prof = cis_df[self.column_dict[qt][1]]['당기순이익'] - cis_df[self.column_dict[qt][2]]['당기순이익']
                        elif len(self.column_dict[qt]) == 4 :
                            prof = cis_df[self.column_dict[qt][1]]['당기순이익']
                        elif len(self.column_dict[qt]) == 3 :
                            prof = cis_df[self.column_dict[qt][1]]['당기순이익']
                    elif '당기순이익(손실)' in cis_df.index :
                        if len(self.column_dict[qt]) == 5 :
                            prof = cis_df[self.column_dict[qt][1]]['당기순이익(손실)'] - cis_df[self.column_dict[qt][2]]['당기순이익(손실)']
                        elif len(self.column_dict[qt]) == 4 :
                            prof = cis_df[self.column_dict[qt][1]]['당기순이익(손실)']
                        elif len(self.column_dict[qt]) == 3 :
                            prof = cis_df[self.column_dict[qt][1]]['당기순이익(손실)']
                    # 전기 당기순이익
                    if '당기순이익' in cis_df.index :
                        if len(self.column_dict[qt]) == 5 :
                            b_prof = cis_df[self.column_dict[qt][3]]['당기순이익']
                        elif len(self.column_dict[qt]) == 4 :
                            b_prof = cis_df[self.column_dict[qt][2]]['당기순이익'] - cis_df[self.column_dict[qt][3]]['당기순이익']
                        elif len(self.column_dict[qt]) == 3 :
                            b_prof = cis_df[self.column_dict[qt][2]]['당기순이익']
                    elif '당기순이익(손실)' in cis_df.index :
                        if len(self.column_dict[qt]) == 5 :
                            b_prof = cis_df[self.column_dict[qt][3]]['당기순이익(손실)']
                        elif len(self.column_dict[qt]) == 4 :
                            b_prof = cis_df[self.column_dict[qt][2]]['당기순이익(손실)'] - cis_df[self.column_dict[qt][3]]['당기순이익(손실)']
                        elif len(self.column_dict[qt]) == 3 :
                            b_prof = cis_df[self.column_dict[qt][2]]['당기순이익(손실)']
                    # 영업이익
                    if '영업이익' in cis_df.index : 
                        if len(self.column_dict[qt]) == 5 :
                            buis_p = cis_df[self.column_dict[qt][1]]['영업이익'] - cis_df[self.column_dict[qt][2]]['영업이익']
                        elif len(self.column_dict[qt]) == 4 :
                            buis_p = cis_df[self.column_dict[qt][1]]['영업이익']
                        elif len(self.column_dict[qt]) == 3 :
                            buis_p = cis_df[self.column_dict[qt][1]]['영업이익']
                    elif '영업이익(손실)' in cis_df.index :
                        if len(self.column_dict[qt]) == 5 :
                            buis_p = cis_df[self.column_dict[qt][1]]['영업이익(손실)'] - cis_df[self.column_dict[qt][2]]['영업이익(손실)']
                        elif len(self.column_dict[qt]) == 4 :
                            buis_p = cis_df[self.column_dict[qt][1]]['영업이익(손실)']
                        elif len(self.column_dict[qt]) == 3 :
                            buis_p = cis_df[self.column_dict[qt][1]]['영업이익(손실)']
                    
                    ## 성장성 ##
                    # 총자산 증가율 = (당기 자산총계 / 전기 자산총계 -1) * 100
                    asst_inc = ((asst / b_asst) -1) * 100
                    # 매출액 증가율 = (당기 매출액 / 전기 매출액 -1) * 100
                    rev_inc = ((rev / b_rev) - 1) * 100
                    # 당기순이익 증가율 = (당기 당기순이익 / 전기 당기순이익 -1) * 100
                    prof_inc = ((prof / b_prof) - 1) * 100
                    # 자기자본 증가율 = (당기 자기자본 / 전기 매출액 -1) * 100
                    s_asst_inc = ((eq / b_rev) - 1) * 100

                    ## 수익성 ##
                    # 매출액 영업이익률 = (영업이익 / 매출액) * 100
                    rev_bpr = (buis_p / rev) * 100
                    # 매출액 순이익률 = (당기순이익 / 매출액) * 100
                    rev_npr = (prof / rev) * 100
                    # 총자본 순이익률 = (당기순이익 / 총자본) * 100
                    eq_npr = (prof / eq) * 100
                    # 경영자산 영업이익률 = (영업이익 / 경영자산) * 100    ; 경영자산 = 운전자본 = (유동자산 - 유동부채)
                    ra_bpr = (buis_p / (r_asset - r_debt)) * 100

                    ## 안정성 ##
                    # 유동비율 = (유동자산 / 유동부채) * 100
                    r_ratio = (r_asset / r_debt) * 100
                    # 당좌비율 = {(유동자산 - 재고자산) / 유동부채} * 100
                    d_ratio = ((r_asset - st_asset) / r_debt) * 100
                    # 고정비율 = 비유동자산 / 자기자본 * 100
                    f_ratio = (nr_asset / eq) * 100
                    # 부채비율 = 부채 / 자기자본 * 100
                    debt_r = (lia / eq) * 100

                    ## 활동성 ##
                    # 총자본 회전율 = 매출액 / 총자본
                    asst_to = rev / eq
                    # 매출채권 회전율 = 매출액 / 매출채권
                    sa_c_to = rev / sa_c
                    # 재고자산 회전율 = 매출액 / 재고자산
                    st_to = rev / st_asset
                    
                    # ROE
                    roe = (prof / eq) * 100

                    # DataFrame에 값 대입
                    fsr_df[qt]['총자산'] = asst
                    fsr_df[qt]['전기총자산'] = b_asst
                    fsr_df[qt]['총자본'] = eq
                    fsr_df[qt]['총부채'] = lia
                    fsr_df[qt]['유동자산'] = r_asset
                    fsr_df[qt]['유동부채'] = r_debt
                    fsr_df[qt]['재고자산'] = st_asset
                    fsr_df[qt]['비유동자산'] = nr_asset
                    fsr_df[qt]['매출채권'] = sa_c
                    fsr_df[qt]['매출액'] = rev
                    fsr_df[qt]['전기매출액'] = b_rev
                    fsr_df[qt]['당기순이익'] = prof
                    fsr_df[qt]['전기당기순이익'] = b_prof
                    fsr_df[qt]['영업이익'] = buis_p
                    fsr_df[qt]['총자산증가율'] = asst_inc
                    fsr_df[qt]['매출액증가율'] = rev_inc
                    fsr_df[qt]['당기순이익증가율'] = prof_inc
                    fsr_df[qt]['자기자본증가율'] = s_asst_inc
                    fsr_df[qt]['매출액영업이익률'] = rev_bpr
                    fsr_df[qt]['매출액순이익률'] = rev_npr
                    fsr_df[qt]['총자본순이익률'] = eq_npr
                    fsr_df[qt]['경영자산영업이익률'] = ra_bpr
                    fsr_df[qt]['유동비율'] = r_ratio
                    fsr_df[qt]['당좌비율'] = d_ratio
                    fsr_df[qt]['고정비율'] = f_ratio
                    fsr_df[qt]['부채비율'] = debt_r
                    fsr_df[qt]['총자본회전율'] = asst_to
                    fsr_df[qt]['매출채권회전율'] = sa_c_to
                    fsr_df[qt]['재고자산회전율'] = st_to
                    fsr_df[qt]['ROE'] = roe

                    print('{} {} Financial Ratio creation complete'.format(fs_name, qt))

                else : 
                    print('{} {} is None'.format(fs_name, qt))
                    pass
           
            #해당 기업의 재무비율 DataFrame을 pickle 파일로 저장
            fsr_df.to_pickle('./dataset/fsr/{}_fsr.pkl'.format(fs_code))
        
    def mapping_fsr(self, df) :
        import os
        import pandas as pd
        from pykrx import stock
        
        # fsr pickle 파일 로드
        file_list = os.listdir('./dataset/fsr/')
        df_list = []
        stock_list = []
        for i, file_name in enumerate(file_list) :
            tmp_df = pd.read_pickle('./dataset/fsr/{}'.format(file_name))
            tc = str(file_name[:-8])
            name = stock.get_market_ticker_name(tc)
            stock_list.append(name)
            df_list.append(tmp_df)
        
        # 종목이름 : 재무비율 df 의 dictionary 생성
        var_dict = dict(zip(stock_list, df_list))
                        
        # 재무비율 mapping 함수 생성
        df.DATE = df.DATE.astype(str)

        def asst_inc(x) :
            for key in list(var_dict.keys()) :
                
                if (x['DATE'] >= '2021-04-01') & (x['DATE'] <= '2021-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20201231']['총자산증가율']
                elif (x['DATE'] >= '2021-01-01') & (x['DATE'] <= '2021-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200930']['총자산증가율']
                elif (x['DATE'] >= '2020-10-01') & (x['DATE'] <= '2020-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200630']['총자산증가율']
                elif (x['DATE'] >= '2020-07-01') & (x['DATE'] <= '2020-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20200331']['총자산증가율']
                elif (x['DATE'] >= '2020-04-01') & (x['DATE'] <= '2020-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20191231']['총자산증가율']
                elif (x['DATE'] >= '2020-01-01') & (x['DATE'] <= '2020-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190930']['총자산증가율']
                elif (x['DATE'] >= '2019-10-01') & (x['DATE'] <= '2019-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190630']['총자산증가율']
                elif (x['DATE'] >= '2019-07-01') & (x['DATE'] <= '2019-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20190331']['총자산증가율']
                elif (x['DATE'] >= '2019-04-01') & (x['DATE'] <= '2019-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20181231']['총자산증가율']
                elif (x['DATE'] >= '2019-01-01') & (x['DATE'] <= '2019-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180930']['총자산증가율']
                elif (x['DATE'] >= '2018-10-01') & (x['DATE'] <= '2018-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180630']['총자산증가율']
                elif (x['DATE'] >= '2018-07-01') & (x['DATE'] <= '2018-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20180331']['총자산증가율']
                elif (x['DATE'] >= '2018-04-01') & (x['DATE'] <= '2018-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20171231']['총자산증가율']
                elif (x['DATE'] >= '2018-01-01') & (x['DATE'] <= '2018-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20170930']['총자산증가율']
        
        def rev_inc(x) :
            for key in list(var_dict.keys()) :
                if (x['DATE'] >= '2021-04-01') & (x['DATE'] <= '2021-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20201231']['매출액증가율']
                elif (x['DATE'] >= '2021-01-01') & (x['DATE'] <= '2021-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200930']['매출액증가율']
                elif (x['DATE'] >= '2020-10-01') & (x['DATE'] <= '2020-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200630']['매출액증가율']
                elif (x['DATE'] >= '2020-07-01') & (x['DATE'] <= '2020-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20200331']['매출액증가율']
                elif (x['DATE'] >= '2020-04-01') & (x['DATE'] <= '2020-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20191231']['매출액증가율']
                elif (x['DATE'] >= '2020-01-01') & (x['DATE'] <= '2020-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190930']['매출액증가율']
                elif (x['DATE'] >= '2019-10-01') & (x['DATE'] <= '2019-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190630']['매출액증가율']
                elif (x['DATE'] >= '2019-07-01') & (x['DATE'] <= '2019-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20190331']['매출액증가율']
                elif (x['DATE'] >= '2019-04-01') & (x['DATE'] <= '2019-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20181231']['매출액증가율']
                elif (x['DATE'] >= '2019-01-01') & (x['DATE'] <= '2019-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180930']['매출액증가율']
                elif (x['DATE'] >= '2018-10-01') & (x['DATE'] <= '2018-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180630']['매출액증가율']
                elif (x['DATE'] >= '2018-07-01') & (x['DATE'] <= '2018-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20180331']['매출액증가율']
                elif (x['DATE'] >= '2018-04-01') & (x['DATE'] <= '2018-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20171231']['매출액증가율']
                elif (x['DATE'] >= '2018-01-01') & (x['DATE'] <= '2018-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20170930']['매출액증가율']
        
        def prof_inc(x) :
            for key in list(var_dict.keys()) :
                if (x['DATE'] >= '2021-04-01') & (x['DATE'] <= '2021-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20201231']['당기순이익증가율']
                elif (x['DATE'] >= '2021-01-01') & (x['DATE'] <= '2021-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200930']['당기순이익증가율']
                elif (x['DATE'] >= '2020-10-01') & (x['DATE'] <= '2020-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200630']['당기순이익증가율']
                elif (x['DATE'] >= '2020-07-01') & (x['DATE'] <= '2020-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20200331']['당기순이익증가율']
                elif (x['DATE'] >= '2020-04-01') & (x['DATE'] <= '2020-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20191231']['당기순이익증가율']
                elif (x['DATE'] >= '2020-01-01') & (x['DATE'] <= '2020-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190930']['당기순이익증가율']
                elif (x['DATE'] >= '2019-10-01') & (x['DATE'] <= '2019-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190630']['당기순이익증가율']
                elif (x['DATE'] >= '2019-07-01') & (x['DATE'] <= '2019-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20190331']['당기순이익증가율']
                elif (x['DATE'] >= '2019-04-01') & (x['DATE'] <= '2019-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20181231']['당기순이익증가율']
                elif (x['DATE'] >= '2019-01-01') & (x['DATE'] <= '2019-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180930']['당기순이익증가율']
                elif (x['DATE'] >= '2018-10-01') & (x['DATE'] <= '2018-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180630']['당기순이익증가율']
                elif (x['DATE'] >= '2018-07-01') & (x['DATE'] <= '2018-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20180331']['당기순이익증가율']
                elif (x['DATE'] >= '2018-04-01') & (x['DATE'] <= '2018-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20171231']['당기순이익증가율']
                elif (x['DATE'] >= '2018-01-01') & (x['DATE'] <= '2018-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20170930']['당기순이익증가율']

        def s_asst_inc(x) :
            for key in list(var_dict.keys()) :
                if (x['DATE'] >= '2021-04-01') & (x['DATE'] <= '2021-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20201231']['자기자본증가율']
                elif (x['DATE'] >= '2021-01-01') & (x['DATE'] <= '2021-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200930']['자기자본증가율']
                elif (x['DATE'] >= '2020-10-01') & (x['DATE'] <= '2020-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200630']['자기자본증가율']
                elif (x['DATE'] >= '2020-07-01') & (x['DATE'] <= '2020-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20200331']['자기자본증가율']
                elif (x['DATE'] >= '2020-04-01') & (x['DATE'] <= '2020-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20191231']['자기자본증가율']
                elif (x['DATE'] >= '2020-01-01') & (x['DATE'] <= '2020-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190930']['자기자본증가율']
                elif (x['DATE'] >= '2019-10-01') & (x['DATE'] <= '2019-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190630']['자기자본증가율']
                elif (x['DATE'] >= '2019-07-01') & (x['DATE'] <= '2019-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20190331']['자기자본증가율']
                elif (x['DATE'] >= '2019-04-01') & (x['DATE'] <= '2019-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20181231']['자기자본증가율']
                elif (x['DATE'] >= '2019-01-01') & (x['DATE'] <= '2019-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180930']['자기자본증가율']
                elif (x['DATE'] >= '2018-10-01') & (x['DATE'] <= '2018-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180630']['자기자본증가율']
                elif (x['DATE'] >= '2018-07-01') & (x['DATE'] <= '2018-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20180331']['자기자본증가율']
                elif (x['DATE'] >= '2018-04-01') & (x['DATE'] <= '2018-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20171231']['자기자본증가율']
                elif (x['DATE'] >= '2018-01-01') & (x['DATE'] <= '2018-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20170930']['자기자본증가율']
        
        def rev_bpr(x) :
            for key in list(var_dict.keys()) :
                if (x['DATE'] >= '2021-04-01') & (x['DATE'] <= '2021-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20201231']['매출액영업이익률']
                elif (x['DATE'] >= '2021-01-01') & (x['DATE'] <= '2021-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200930']['매출액영업이익률']
                elif (x['DATE'] >= '2020-10-01') & (x['DATE'] <= '2020-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200630']['매출액영업이익률']
                elif (x['DATE'] >= '2020-07-01') & (x['DATE'] <= '2020-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20200331']['매출액영업이익률']
                elif (x['DATE'] >= '2020-04-01') & (x['DATE'] <= '2020-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20191231']['매출액영업이익률']
                elif (x['DATE'] >= '2020-01-01') & (x['DATE'] <= '2020-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190930']['매출액영업이익률']
                elif (x['DATE'] >= '2019-10-01') & (x['DATE'] <= '2019-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190630']['매출액영업이익률']
                elif (x['DATE'] >= '2019-07-01') & (x['DATE'] <= '2019-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20190331']['매출액영업이익률']
                elif (x['DATE'] >= '2019-04-01') & (x['DATE'] <= '2019-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20181231']['매출액영업이익률']
                elif (x['DATE'] >= '2019-01-01') & (x['DATE'] <= '2019-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180930']['매출액영업이익률']
                elif (x['DATE'] >= '2018-10-01') & (x['DATE'] <= '2018-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180630']['매출액영업이익률']
                elif (x['DATE'] >= '2018-07-01') & (x['DATE'] <= '2018-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20180331']['매출액영업이익률']
                elif (x['DATE'] >= '2018-04-01') & (x['DATE'] <= '2018-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20171231']['매출액영업이익률']
                elif (x['DATE'] >= '2018-01-01') & (x['DATE'] <= '2018-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20170930']['매출액영업이익률']
        
        def eq_npr(x) :
            for key in list(var_dict.keys()) :
                if (x['DATE'] >= '2021-04-01') & (x['DATE'] <= '2021-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20201231']['총자본순이익률']
                elif (x['DATE'] >= '2021-01-01') & (x['DATE'] <= '2021-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200930']['총자본순이익률']
                elif (x['DATE'] >= '2020-10-01') & (x['DATE'] <= '2020-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200630']['총자본순이익률']
                elif (x['DATE'] >= '2020-07-01') & (x['DATE'] <= '2020-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20200331']['총자본순이익률']
                elif (x['DATE'] >= '2020-04-01') & (x['DATE'] <= '2020-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20191231']['총자본순이익률']
                elif (x['DATE'] >= '2020-01-01') & (x['DATE'] <= '2020-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190930']['총자본순이익률']
                elif (x['DATE'] >= '2019-10-01') & (x['DATE'] <= '2019-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190630']['총자본순이익률']
                elif (x['DATE'] >= '2019-07-01') & (x['DATE'] <= '2019-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20190331']['총자본순이익률']
                elif (x['DATE'] >= '2019-04-01') & (x['DATE'] <= '2019-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20181231']['총자본순이익률']
                elif (x['DATE'] >= '2019-01-01') & (x['DATE'] <= '2019-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180930']['총자본순이익률']
                elif (x['DATE'] >= '2018-10-01') & (x['DATE'] <= '2018-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180630']['총자본순이익률']
                elif (x['DATE'] >= '2018-07-01') & (x['DATE'] <= '2018-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20180331']['총자본순이익률']
                elif (x['DATE'] >= '2018-04-01') & (x['DATE'] <= '2018-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20171231']['총자본순이익률']
                elif (x['DATE'] >= '2018-01-01') & (x['DATE'] <= '2018-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20170930']['총자본순이익률']
        
        def ra_bpr(x) :
            for key in list(var_dict.keys()) :
                if (x['DATE'] >= '2021-04-01') & (x['DATE'] <= '2021-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20201231']['경영자산영업이익률']
                elif (x['DATE'] >= '2021-01-01') & (x['DATE'] <= '2021-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200930']['경영자산영업이익률']
                elif (x['DATE'] >= '2020-10-01') & (x['DATE'] <= '2020-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200630']['경영자산영업이익률']
                elif (x['DATE'] >= '2020-07-01') & (x['DATE'] <= '2020-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20200331']['경영자산영업이익률']
                elif (x['DATE'] >= '2020-04-01') & (x['DATE'] <= '2020-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20191231']['경영자산영업이익률']
                elif (x['DATE'] >= '2020-01-01') & (x['DATE'] <= '2020-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190930']['경영자산영업이익률']
                elif (x['DATE'] >= '2019-10-01') & (x['DATE'] <= '2019-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190630']['경영자산영업이익률']
                elif (x['DATE'] >= '2019-07-01') & (x['DATE'] <= '2019-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20190331']['경영자산영업이익률']
                elif (x['DATE'] >= '2019-04-01') & (x['DATE'] <= '2019-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20181231']['경영자산영업이익률']
                elif (x['DATE'] >= '2019-01-01') & (x['DATE'] <= '2019-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180930']['경영자산영업이익률']
                elif (x['DATE'] >= '2018-10-01') & (x['DATE'] <= '2018-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180630']['경영자산영업이익률']
                elif (x['DATE'] >= '2018-07-01') & (x['DATE'] <= '2018-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20180331']['경영자산영업이익률']
                elif (x['DATE'] >= '2018-04-01') & (x['DATE'] <= '2018-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20171231']['경영자산영업이익률']
                elif (x['DATE'] >= '2018-01-01') & (x['DATE'] <= '2018-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20170930']['경영자산영업이익률']
                    
        def r_ratio(x) :
            for key in list(var_dict.keys()) :
                if (x['DATE'] >= '2021-04-01') & (x['DATE'] <= '2021-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20201231']['유동비율']
                elif (x['DATE'] >= '2021-01-01') & (x['DATE'] <= '2021-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200930']['유동비율']
                elif (x['DATE'] >= '2020-10-01') & (x['DATE'] <= '2020-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200630']['유동비율']
                elif (x['DATE'] >= '2020-07-01') & (x['DATE'] <= '2020-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20200331']['유동비율']
                elif (x['DATE'] >= '2020-04-01') & (x['DATE'] <= '2020-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20191231']['유동비율']
                elif (x['DATE'] >= '2020-01-01') & (x['DATE'] <= '2020-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190930']['유동비율']
                elif (x['DATE'] >= '2019-10-01') & (x['DATE'] <= '2019-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190630']['유동비율']
                elif (x['DATE'] >= '2019-07-01') & (x['DATE'] <= '2019-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20190331']['유동비율']
                elif (x['DATE'] >= '2019-04-01') & (x['DATE'] <= '2019-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20181231']['유동비율']
                elif (x['DATE'] >= '2019-01-01') & (x['DATE'] <= '2019-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180930']['유동비율']
                elif (x['DATE'] >= '2018-10-01') & (x['DATE'] <= '2018-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180630']['유동비율']
                elif (x['DATE'] >= '2018-07-01') & (x['DATE'] <= '2018-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20180331']['유동비율']
                elif (x['DATE'] >= '2018-04-01') & (x['DATE'] <= '2018-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20171231']['유동비율']
                elif (x['DATE'] >= '2018-01-01') & (x['DATE'] <= '2018-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20170930']['유동비율']
        def d_ratio(x) :
            for key in list(var_dict.keys()) :
                if (x['DATE'] >= '2021-04-01') & (x['DATE'] <= '2021-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20201231']['당좌비율']
                elif (x['DATE'] >= '2021-01-01') & (x['DATE'] <= '2021-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200930']['당좌비율']
                elif (x['DATE'] >= '2020-10-01') & (x['DATE'] <= '2020-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200630']['당좌비율']
                elif (x['DATE'] >= '2020-07-01') & (x['DATE'] <= '2020-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20200331']['당좌비율']
                elif (x['DATE'] >= '2020-04-01') & (x['DATE'] <= '2020-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20191231']['당좌비율']
                elif (x['DATE'] >= '2020-01-01') & (x['DATE'] <= '2020-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190930']['당좌비율']
                elif (x['DATE'] >= '2019-10-01') & (x['DATE'] <= '2019-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190630']['당좌비율']
                elif (x['DATE'] >= '2019-07-01') & (x['DATE'] <= '2019-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20190331']['당좌비율']
                elif (x['DATE'] >= '2019-04-01') & (x['DATE'] <= '2019-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20181231']['당좌비율']
                elif (x['DATE'] >= '2019-01-01') & (x['DATE'] <= '2019-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180930']['당좌비율']
                elif (x['DATE'] >= '2018-10-01') & (x['DATE'] <= '2018-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180630']['당좌비율']
                elif (x['DATE'] >= '2018-07-01') & (x['DATE'] <= '2018-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20180331']['당좌비율']
                elif (x['DATE'] >= '2018-04-01') & (x['DATE'] <= '2018-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20171231']['당좌비율']
                elif (x['DATE'] >= '2018-01-01') & (x['DATE'] <= '2018-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20170930']['당좌비율']
        
        def f_ratio(x) :
            for key in list(var_dict.keys()) :
                if (x['DATE'] >= '2021-04-01') & (x['DATE'] <= '2021-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20201231']['고정비율']
                elif (x['DATE'] >= '2021-01-01') & (x['DATE'] <= '2021-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200930']['고정비율']
                elif (x['DATE'] >= '2020-10-01') & (x['DATE'] <= '2020-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200630']['고정비율']
                elif (x['DATE'] >= '2020-07-01') & (x['DATE'] <= '2020-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20200331']['고정비율']
                elif (x['DATE'] >= '2020-04-01') & (x['DATE'] <= '2020-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20191231']['고정비율']
                elif (x['DATE'] >= '2020-01-01') & (x['DATE'] <= '2020-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190930']['고정비율']
                elif (x['DATE'] >= '2019-10-01') & (x['DATE'] <= '2019-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190630']['고정비율']
                elif (x['DATE'] >= '2019-07-01') & (x['DATE'] <= '2019-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20190331']['고정비율']
                elif (x['DATE'] >= '2019-04-01') & (x['DATE'] <= '2019-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20181231']['고정비율']
                elif (x['DATE'] >= '2019-01-01') & (x['DATE'] <= '2019-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180930']['고정비율']
                elif (x['DATE'] >= '2018-10-01') & (x['DATE'] <= '2018-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180630']['고정비율']
                elif (x['DATE'] >= '2018-07-01') & (x['DATE'] <= '2018-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20180331']['고정비율']
                elif (x['DATE'] >= '2018-04-01') & (x['DATE'] <= '2018-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20171231']['고정비율']
                elif (x['DATE'] >= '2018-01-01') & (x['DATE'] <= '2018-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20170930']['고정비율']
        
        def debt_r(x) :
            for key in list(var_dict.keys()) :
                if (x['DATE'] >= '2021-04-01') & (x['DATE'] <= '2021-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20201231']['부채비율']
                elif (x['DATE'] >= '2021-01-01') & (x['DATE'] <= '2021-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200930']['부채비율']
                elif (x['DATE'] >= '2020-10-01') & (x['DATE'] <= '2020-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200630']['부채비율']
                elif (x['DATE'] >= '2020-07-01') & (x['DATE'] <= '2020-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20200331']['부채비율']
                elif (x['DATE'] >= '2020-04-01') & (x['DATE'] <= '2020-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20191231']['부채비율']
                elif (x['DATE'] >= '2020-01-01') & (x['DATE'] <= '2020-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190930']['부채비율']
                elif (x['DATE'] >= '2019-10-01') & (x['DATE'] <= '2019-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190630']['부채비율']
                elif (x['DATE'] >= '2019-07-01') & (x['DATE'] <= '2019-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20190331']['부채비율']
                elif (x['DATE'] >= '2019-04-01') & (x['DATE'] <= '2019-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20181231']['부채비율']
                elif (x['DATE'] >= '2019-01-01') & (x['DATE'] <= '2019-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180930']['부채비율']
                elif (x['DATE'] >= '2018-10-01') & (x['DATE'] <= '2018-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180630']['부채비율']
                elif (x['DATE'] >= '2018-07-01') & (x['DATE'] <= '2018-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20180331']['부채비율']
                elif (x['DATE'] >= '2018-04-01') & (x['DATE'] <= '2018-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20171231']['부채비율']
                elif (x['DATE'] >= '2018-01-01') & (x['DATE'] <= '2018-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20170930']['부채비율']
        
        def asst_to(x) :
            for key in list(var_dict.keys()) :
                if (x['DATE'] >= '2021-04-01') & (x['DATE'] <= '2021-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20201231']['총자본회전율']
                elif (x['DATE'] >= '2021-01-01') & (x['DATE'] <= '2021-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200930']['총자본회전율']
                elif (x['DATE'] >= '2020-10-01') & (x['DATE'] <= '2020-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200630']['총자본회전율']
                elif (x['DATE'] >= '2020-07-01') & (x['DATE'] <= '2020-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20200331']['총자본회전율']
                elif (x['DATE'] >= '2020-04-01') & (x['DATE'] <= '2020-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20191231']['총자본회전율']
                elif (x['DATE'] >= '2020-01-01') & (x['DATE'] <= '2020-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190930']['총자본회전율']
                elif (x['DATE'] >= '2019-10-01') & (x['DATE'] <= '2019-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190630']['총자본회전율']
                elif (x['DATE'] >= '2019-07-01') & (x['DATE'] <= '2019-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20190331']['총자본회전율']
                elif (x['DATE'] >= '2019-04-01') & (x['DATE'] <= '2019-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20181231']['총자본회전율']
                elif (x['DATE'] >= '2019-01-01') & (x['DATE'] <= '2019-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180930']['총자본회전율']
                elif (x['DATE'] >= '2018-10-01') & (x['DATE'] <= '2018-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180630']['총자본회전율']
                elif (x['DATE'] >= '2018-07-01') & (x['DATE'] <= '2018-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20180331']['총자본회전율']
                elif (x['DATE'] >= '2018-04-01') & (x['DATE'] <= '2018-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20171231']['총자본회전율']
                elif (x['DATE'] >= '2018-01-01') & (x['DATE'] <= '2018-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20170930']['총자본회전율']
        
        def sa_c_to(x) :
            for key in list(var_dict.keys()) :
                if (x['DATE'] >= '2021-04-01') & (x['DATE'] <= '2021-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20201231']['매출채권회전율']
                elif (x['DATE'] >= '2021-01-01') & (x['DATE'] <= '2021-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200930']['매출채권회전율']
                elif (x['DATE'] >= '2020-10-01') & (x['DATE'] <= '2020-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200630']['매출채권회전율']
                elif (x['DATE'] >= '2020-07-01') & (x['DATE'] <= '2020-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20200331']['매출채권회전율']
                elif (x['DATE'] >= '2020-04-01') & (x['DATE'] <= '2020-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20191231']['매출채권회전율']
                elif (x['DATE'] >= '2020-01-01') & (x['DATE'] <= '2020-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190930']['매출채권회전율']
                elif (x['DATE'] >= '2019-10-01') & (x['DATE'] <= '2019-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190630']['매출채권회전율']
                elif (x['DATE'] >= '2019-07-01') & (x['DATE'] <= '2019-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20190331']['매출채권회전율']
                elif (x['DATE'] >= '2019-04-01') & (x['DATE'] <= '2019-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20181231']['매출채권회전율']
                elif (x['DATE'] >= '2019-01-01') & (x['DATE'] <= '2019-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180930']['매출채권회전율']
                elif (x['DATE'] >= '2018-10-01') & (x['DATE'] <= '2018-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180630']['매출채권회전율']
                elif (x['DATE'] >= '2018-07-01') & (x['DATE'] <= '2018-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20180331']['매출채권회전율']
                elif (x['DATE'] >= '2018-04-01') & (x['DATE'] <= '2018-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20171231']['매출채권회전율']
                elif (x['DATE'] >= '2018-01-01') & (x['DATE'] <= '2018-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20170930']['매출채권회전율']
        
        def st_to(x) :
            for key in list(var_dict.keys()) :
                if (x['DATE'] >= '2021-04-01') & (x['DATE'] <= '2021-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20201231']['재고자산회전율']
                elif (x['DATE'] >= '2021-01-01') & (x['DATE'] <= '2021-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200930']['재고자산회전율']
                elif (x['DATE'] >= '2020-10-01') & (x['DATE'] <= '2020-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20200630']['재고자산회전율']
                elif (x['DATE'] >= '2020-07-01') & (x['DATE'] <= '2020-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20200331']['재고자산회전율']
                elif (x['DATE'] >= '2020-04-01') & (x['DATE'] <= '2020-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20191231']['재고자산회전율']
                elif (x['DATE'] >= '2020-01-01') & (x['DATE'] <= '2020-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190930']['재고자산회전율']
                elif (x['DATE'] >= '2019-10-01') & (x['DATE'] <= '2019-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20190630']['재고자산회전율']
                elif (x['DATE'] >= '2019-07-01') & (x['DATE'] <= '2019-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20190331']['재고자산회전율']
                elif (x['DATE'] >= '2019-04-01') & (x['DATE'] <= '2019-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20181231']['재고자산회전율']
                elif (x['DATE'] >= '2019-01-01') & (x['DATE'] <= '2019-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180930']['재고자산회전율']
                elif (x['DATE'] >= '2018-10-01') & (x['DATE'] <= '2018-12-31') & (x['NAME'] == key) :
                    return var_dict[key]['20180630']['재고자산회전율']
                elif (x['DATE'] >= '2018-07-01') & (x['DATE'] <= '2018-09-30') & (x['NAME'] == key) :
                    return var_dict[key]['20180331']['재고자산회전율']
                elif (x['DATE'] >= '2018-04-01') & (x['DATE'] <= '2018-06-30') & (x['NAME'] == key) :
                    return var_dict[key]['20171231']['재고자산회전율']
                elif (x['DATE'] >= '2018-01-01') & (x['DATE'] <= '2018-03-31') & (x['NAME'] == key) :
                    return var_dict[key]['20170930']['재고자산회전율']
        
        # 재무비율 mapping
        df['ASST_INC'] = df.apply(asst_inc,axis=1)
        print('ASST_INC Mapping Complete')
        df['REV_INC'] = df.apply(rev_inc,axis=1)
        print('REV_INC Mapping Complete')
        df['PROF_INC'] = df.apply(prof_inc,axis=1)
        print('PROF_INC Mapping Complete')
        df['S_ASST_INC'] = df.apply(s_asst_inc,axis=1)
        print('S_ASST_INC Mapping Complete')
        df['REV_BPR'] = df.apply(rev_bpr,axis=1)
        print('REV_BPR Mapping Complete')
        df['EQ_NPR'] = df.apply(eq_npr,axis=1)
        print('EQ_NPR Mapping Complete')
        df['RA_BPR'] = df.apply(ra_bpr,axis=1)
        print('RA_BPR Mapping Complete')
        df['R_RATIO'] = df.apply(r_ratio,axis=1)
        print('R_RATIO Mapping Complete')
        df['D_RATIO'] = df.apply(d_ratio,axis=1)
        print('D_RATIO Mapping Complete')
        df['F_RATIO'] = df.apply(f_ratio,axis=1)
        print('F_RATIO Mapping Complete')
        df['DEBT_R'] = df.apply(debt_r,axis=1)
        print('DEBT_R Mapping Complete')
        df['ASST_TO'] = df.apply(asst_to,axis=1)
        print('ASST_TO Mapping Complete')
        df['SA_C_TO'] = df.apply(sa_c_to,axis=1)
        print('SA_C_TO Mapping Complete')
        df['ST_TO'] = df.apply(st_to,axis=1)
        print('ST_TO Mapping Complete')

        # df 저장
        from datetime import datetime as dt
        now = dt.now()
        now_str = now.strftime('%m%d_%H')
        df.to_pickle('./dataset/QAdf_v{}.pkl'.format(now_str))

        return df




##### 기술적 지표 생성 class #####
class TechnicalAnalysis :
    
    def __init__(self, df) :
        self.df = df
        
    # 사용자 함수 : 기술적 지표를 생성하여 DataFrame에 삽입
    def get_TA(self) :
        
        import pandas as pd
        import talib.abstract as ta
        
        df = self.df
        
        df['MA5'] = 0
        df['MA10'] = 0
        df['MA20'] = 0
        df['MA50'] = 0
        df['ADX'] = 0
        df['CCI'] = 0
        df['WILLR'] = 0
        df['RSI'] = 0
        
        def insert(metric, column_name, stock_name) :
            for idx in metric.index :
                mask = (df.NAME == stock_name) & (df.DATE == idx)
                i = df[mask].index[0]
                df.loc[i, column_name] = metric[idx]
        
        name_list = df.NAME.unique().tolist()
        for stock_name in name_list :
            mask = df.NAME == stock_name
            ohlc_df = df[mask][['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE']]
            ohlc_df.rename({'DATE' : 'date', 'OPEN' : 'open', 'HIGH' : 'high', 'LOW' : 'low', 'CLOSE' : 'close'}, axis=1, inplace=True)
            ohlc_df.set_index('date', inplace=True)
            
            ### 추세지표 ###
            # 5일 이평선
            ma5 = ta.MA(ohlc_df, timeperiod=5)
            insert(ma5, 'MA5', stock_name)
            # 10일 이평선
            ma10 = ta.MA(ohlc_df, timeperiod=10)
            insert(ma10, 'MA10', stock_name)
            # 20일 이평선
            ma20 = ta.MA(ohlc_df, timeperiod=20)
            insert(ma20, 'MA20', stock_name)
            # 50일 이평선
            ma50 = ta.MA(ohlc_df, timeperiod=50)
            insert(ma50, 'MA50', stock_name)
            
            ### 모멘텀지표 ###
            # ADX
            adx = ta.ADX(ohlc_df, 14)
            insert(adx, 'ADX', stock_name)
            # CCI
            cci = ta.CCI(ohlc_df, 14)
            insert(cci, 'CCI', stock_name)
            # Williams%R
            willr = ta.WILLR(ohlc_df, 14)
            insert(willr, 'WILLR', stock_name)
            
            ### 시장강도 ###
            # RSI
            rsi = ta.RSI(ohlc_df, 14)
            insert(rsi, 'RSI', stock_name)
        
        return df
    
    









