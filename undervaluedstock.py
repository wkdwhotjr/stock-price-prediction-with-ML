### 사용자 함수 : 매출액 크롤링 ###
def get_total_sales(ticker):
    from bs4 import BeautifulSoup
    import requests
    import re
    URL = "https://finance.naver.com/item/main.nhn?code="+ticker
    result = requests.get(URL)
    soup = BeautifulSoup(result.text, "html.parser")
    k = soup.find_all("div",{"class":'section cop_analysis'})[0]
    one = k.find_all("td",{"class":""})[7]  #2021년 1분기
    one = re.sub("<.+>",'',str(one))
    one = re.sub("\t",'',one)
    one = re.sub("\n",'',one)
    four = k.find_all("td",{"class":""})[6]  #2020년 4분기
    four = re.sub("<.+>",'',str(four))  
    four = re.sub("\t",'',four)
    four = re.sub("\n",'',four)
    three = k.find_all("td",{"class":""})[5]  #2020년 3분기
    three = re.sub("<.+>",'',str(three))  
    three = re.sub("\t",'',three)
    three = re.sub("\n",'',three)
    two = k.find_all("td",{"class":""})[4]  #2020년 2분기
    two = re.sub("<.+>",'',str(two))  
    two = re.sub("\t",'',two)
    two = re.sub("\n",'',two)
    
    def make_int(q) : 
        q_tmp = q.split(',')
        if len(q_tmp) == 3 :
            return int(q_tmp[0] + q_tmp[1] + q_tmp[2])
        elif len(q_tmp) == 2 :
            return int(q_tmp[0] + q_tmp[1])
        elif len(q_tmp) == 1 :
            return int(q_tmp[0])

    o = make_int(one)
    f = make_int(four)
    th = make_int(three)
    tw = make_int(two)

    return o, f, th, tw



### 사용자 함수 : 저평가주 DataFrame 생성 ###
def find_undervalued_stock(last_business_day=None, day=None) :
    from pykrx import stock
    import pandas as pd
    import datetime
    import pickle
    import os

    with open('./dataset/N_MTS_dict.pkl', 'rb') as f :
        n_mts_dict = pickle.load(f)

    files = os.listdir('./fsdata/')
    code_list = []
    for file_name in files :
        code = file_name[:-5]
        code_list.append(code)
    
    # 날짜를 입력하지 않고 last_business_day = True를 선택하는 경우 : 마지막 영업일 기준 산출
    if (last_business_day == True) & (day == None) :
        yesterday = datetime.datetime.today() - datetime.timedelta(1)
        yesterday_str = yesterday.strftime('%Y%m%d')
        yesterday_df = stock.get_market_fundamental_by_ticker(yesterday_str, market='KOSPI')
        
        # 어제가 마지막 영업일이 맞는 경우 : 그대로 진행
        if yesterday_df.PER.mean() != 0 :
            
            drop_idx_list = []
            for idx in yesterday_df.index :
                if idx not in code_list :
                    drop_idx_list.append(idx)
            yesterday_df.drop(drop_idx_list, axis=0, inplace=True)
            
            name_list = []
            for ticker in yesterday_df.index :
                name = stock.get_market_ticker_name(ticker)
                name_list.append(name)
            yesterday_df['NAME'] = name_list

            sector_list = []
            for name in name_list :
                sector = n_mts_dict[name][2]
                sector_list.append(sector)
            yesterday_df['SECTOR'] = sector_list

            def per_sector(x) :
                mask = yesterday_df.SECTOR == x['SECTOR']
                return yesterday_df[mask]['PER'].mean()
            def pbr_sector(x) :
                mask = yesterday_df.SECTOR == x['SECTOR']
                return yesterday_df[mask]['PBR'].mean()
            yesterday_df['PER(SECTOR)'] = yesterday_df.apply(per_sector, axis=1)
            yesterday_df['PBR(SECTOR)'] = yesterday_df.apply(pbr_sector, axis=1)

            yesterday_df['ROE'] = yesterday_df['PER'] / yesterday_df['PBR']
            
            yesterday_df['SALES_INCREASING_RATIO'] = 0
            for ticker in yesterday_df.index :
                one, four, three, two = get_total_sales(ticker)
                if (yesterday_str >= '20210401') & (yesterday_str <= '20210630') :
                    yesterday_df.loc[ticker, 'SALES_INCREASING_RATIO'] = ((four - three) / three) * 100
                elif (yesterday_str >= '20210101') & (yesterday_str <= '20210331') :
                    yesterday_df.loc[ticker, 'SALES_INCREASING_RATIO'] = ((three - two) / two) * 100

            undervalue_filter = (yesterday_df.PER < yesterday_df['PER(SECTOR)']) & (yesterday_df.PER > 0) & (yesterday_df.PER <= 10) & (yesterday_df.ROE >= 5) & (yesterday_df.ROE <= 20) & (yesterday_df.PBR < yesterday_df['PBR(SECTOR)']) & (yesterday_df.PBR > 0) & (yesterday_df.PBR <= 1) & (yesterday_df.SALES_INCREASING_RATIO > 0)

            result_df = yesterday_df[undervalue_filter][['NAME', 'SECTOR', 'PER', 'PER(SECTOR)', 'PBR', 'PBR(SECTOR)', 'ROE']]
            return result_df
        
        # 어제가 마지막 영업일이 아닌 경우 : 가장 최근 영업일을 기준으로 산출       
        else : 
            for n in range(1,6) :
                business_day = yesterday - datetime.timedelta(n)
                business_day_str = business_day.strftime('%Y%m%d')
                business_day_df = stock.get_market_fundamental_by_ticker(business_day_str, market='KOSPI')
                if business_day_df.PER.mean() != 0 :
                    break
            
            drop_idx_list = []
            for idx in business_day_df.index :
                if idx not in code_list :
                    drop_idx_list.append(idx)
            business_day_df.drop(drop_idx_list, axis=0, inplace=True)
                                 
            name_list = []
            for ticker in business_day_df.index :
                name = stock.get_market_ticker_name(ticker)
                name_list.append(name)
            business_day_df['NAME'] = name_list

            sector_list = []
            for name in name_list :
                sector = n_mts_dict[name][2]
                sector_list.append(sector)
            business_day_df['SECTOR'] = sector_list

            def per_sector(x) :
                mask = business_day_df.SECTOR == x['SECTOR']
                return business_day_df[mask]['PER'].mean()
            def pbr_sector(x) :
                mask = business_day_df.SECTOR == x['SECTOR']
                return business_day_df[mask]['PBR'].mean()
            business_day_df['PER(SECTOR)'] = business_day_df.apply(per_sector, axis=1)
            business_day_df['PBR(SECTOR)'] = business_day_df.apply(pbr_sector, axis=1)

            business_day_df['ROE'] = business_day_df['PER'] / business_day_df['PBR']
            
            business_day_df['SALES_INCREASING_RATIO'] = 0
            for ticker in business_day_df.index :
                one, four, three, two = get_total_sales(ticker)
                if (business_day_str >= '20210401') & (business_day_str <= '20210630') :
                    business_day_df.loc[ticker, 'SALES_INCREASING_RATIO'] = ((four - three) / three) * 100
                elif (business_day_str >= '20210101') & (business_day_str <= '20210331') :
                    business_day_df.loc[ticker, 'SALES_INCREASING_RATIO'] = ((three - two) / two) * 100
            
            undervalue_filter = (business_day_df.PER < business_day_df['PER(SECTOR)']) & (business_day_df.PER > 0) & (business_day_df.PER <= 10) & (business_day_df.ROE >= 5) & (business_day_df.ROE <= 20) & (business_day_df.PBR < business_day_df['PBR(SECTOR)']) & (business_day_df.PBR > 0) & (business_day_df.PBR <= 1) & (business_day_df.SALES_INCREASING_RATIO > 0)
            result_df = business_day_df[undervalue_filter][['NAME', 'SECTOR', 'PER', 'PER(SECTOR)', 'PBR', 'PBR(SECTOR)', 'ROE', 'SALES_INCREASING_RATIO']]
            
            return result_df
    
    elif (last_business_day == None) & (type(day) == str) :
        
        yesterday = datetime.datetime.strptime(day, '%Y%m%d') - datetime.timedelta(1)
        yesterday_str = yesterday.strftime('%Y%m%d')
        yesterday_df = stock.get_market_fundamental_by_ticker(yesterday_str, market='KOSPI')
        
        # 어제가 마지막 영업일이 맞는 경우 : 그대로 진행
        if yesterday_df.PER.mean() != 0 :
            
            drop_idx_list = []
            for idx in yesterday_df.index :
                if idx not in code_list :
                    drop_idx_list.append(idx)
            yesterday_df.drop(drop_idx_list, axis=0, inplace=True)
            
            name_list = []
            for ticker in yesterday_df.index :
                name = stock.get_market_ticker_name(ticker)
                name_list.append(name)
            yesterday_df['NAME'] = name_list

            sector_list = []
            for name in name_list :
                sector = n_mts_dict[name][2]
                sector_list.append(sector)
            yesterday_df['SECTOR'] = sector_list

            def per_sector(x) :
                mask = yesterday_df.SECTOR == x['SECTOR']
                return yesterday_df[mask]['PER'].mean()
            def pbr_sector(x) :
                mask = yesterday_df.SECTOR == x['SECTOR']
                return yesterday_df[mask]['PBR'].mean()
            yesterday_df['PER(SECTOR)'] = yesterday_df.apply(per_sector, axis=1)
            yesterday_df['PBR(SECTOR)'] = yesterday_df.apply(pbr_sector, axis=1)

            yesterday_df['ROE'] = yesterday_df['PER'] / yesterday_df['PBR']
            
            undervalue_filter = (yesterday_df.PER < yesterday_df['PER(SECTOR)']) & (yesterday_df.PER > 0) & (yesterday_df.PER <= 10) & (yesterday_df.ROE >= 5) & (yesterday_df.ROE <= 20) & (yesterday_df.PBR < yesterday_df['PBR(SECTOR)']) & (yesterday_df.PBR > 0) & (yesterday_df.PBR <= 1)

            result_df = yesterday_df[undervalue_filter][['NAME', 'SECTOR', 'PER', 'PER(SECTOR)', 'PBR', 'PBR(SECTOR)', 'ROE']]
            return result_df
        
        # 어제가 마지막 영업일이 아닌 경우 : 가장 최근 영업일을 기준으로 산출       
        else : 
            for n in range(1,6) :
                business_day = yesterday - datetime.timedelta(n)
                business_day_str = business_day.strftime('%Y%m%d')
                business_day_df = stock.get_market_fundamental_by_ticker(business_day_str, market='KOSPI')
                if business_day_df.PER.mean() != 0 :
                    break
            
            drop_idx_list = []
            for idx in business_day_df.index :
                if idx not in code_list :
                    drop_idx_list.append(idx)
            business_day_df.drop(drop_idx_list, axis=0, inplace=True)
                                 
            name_list = []
            for ticker in business_day_df.index :
                name = stock.get_market_ticker_name(ticker)
                name_list.append(name)
            business_day_df['NAME'] = name_list

            sector_list = []
            for name in name_list :
                sector = n_mts_dict[name][2]
                sector_list.append(sector)
            business_day_df['SECTOR'] = sector_list

            def per_sector(x) :
                mask = business_day_df.SECTOR == x['SECTOR']
                return business_day_df[mask]['PER'].mean()
            def pbr_sector(x) :
                mask = business_day_df.SECTOR == x['SECTOR']
                return business_day_df[mask]['PBR'].mean()
            business_day_df['PER(SECTOR)'] = business_day_df.apply(per_sector, axis=1)
            business_day_df['PBR(SECTOR)'] = business_day_df.apply(pbr_sector, axis=1)

            business_day_df['ROE'] = business_day_df['PER'] / business_day_df['PBR']
            
            undervalue_filter = (business_day_df.PER < business_day_df['PER(SECTOR)']) & (business_day_df.PER > 0) & (business_day_df.PER <= 10) & (business_day_df.ROE >= 5) & (business_day_df.ROE <= 20) & (business_day_df.PBR < business_day_df['PBR(SECTOR)']) & (business_day_df.PBR > 0) & (business_day_df.PBR <= 1)
            result_df = business_day_df[undervalue_filter][['NAME', 'SECTOR', 'PER', 'PER(SECTOR)', 'PBR', 'PBR(SECTOR)', 'ROE', 'SALES_INCREASING_RATIO']]
            
            return result_df

