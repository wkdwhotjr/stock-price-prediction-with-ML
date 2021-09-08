# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import requests
import multiprocessing
import os
from time import sleep

def crawl(row: pd.Series, stockName: str):
    try:
        URL = row["URL"]
        date = row["date"]
        #print(f"fetching (date:{date}) {URL}")
        headers = {"authority": "finance.naver.com","user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36", "referer": "finance.naver.com"}
        response = requests.get(URL, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")
        title = soup.select("#content > div.section.inner_sub > table.view > tr > th > strong.c")[0].text # 글 제목
        content = soup.select("#content > div.section.inner_sub > table.view > tr > td > div > div#body")[0].text # 글 
        return pd.Series({"stock_name": stockName, "date": f"{date.year}-{date.month}-{date.day}","title": title, "content": content})
    except Exception as e:
        print(type(e), e)
        return

def download(tasks: pd.DataFrame, saveDir: str, stockName: str, queue):
    
    if len(tasks) == 0: 
        #queue.put(None)
        return
    from_ = datetime.utcfromtimestamp(tasks.date.unique().min().tolist()/1e9)
    to = datetime.utcfromtimestamp(tasks.date.unique().max().tolist()/1e9)
    
    bbs = tasks.apply(lambda x: crawl(x, stockName), axis=1)
    
    try:
        bbs.to_csv(os.path.join(saveDir, f"{stockName} {from_.year}-{from_.month}-{from_.day} to {to.year}-{to.month}-{to.day}.csv"), encoding="cp949")

    except UnicodeEncodeError:
        bbs.to_csv(os.path.join(saveDir, f"{stockName} {from_.year}-{from_.month}-{from_.day} to {to.year}-{to.month}-{to.day}.csv"), encoding="utf8")
    
    #queue.put(bbs)

if __name__ == "__main__":
    ################## KOSPI 시총 상위 25개 종목의 종목 코드 부르기 #################
    stockDF = pd.read_csv("상장법인목록.csv", encoding="cp949")
    
    #####데탑#####
    #stockNames=['DSR', 'GS건설', 'KPX케미칼', 'SK디스커버리', 'STX엔진', '계룡건설산업', '광주신세계']
    #stockNames=['국도화학', '극동유화', '넥센', '넥센타이어', '대유플러스', '동부건설', '동아지질', '디씨엠']
    #stockNames=[ '디티알오토모티브', '롯데정밀화학', '미창석유공업', '백광산업', '삼영무역', '삼호개발', '세이브존I&C',] 
    #stockNames=['유니드', '유성기업', '제일연마', '케이씨', '태경산업', '태광산업', '태영건설', '한국프랜지공업']
    #stockNames=['한신공영', '화승코퍼레이션', '황금에스티']
    stockNames = ['STX엔진',]
    #'태영건설', '광주신세계', '롯데정밀화학', '유성기업'
    #stockNames.remove(os.listdir("종토방"))
    for i in stockNames:
        try:
            stockDF.loc[stockDF["회사명"]==i, "종목코드"].values[0]
        except IndexError:
            print("종목 코드 찾기 실패:", i)


    ################# 종토방 글 목록 불러오기 #################
    #날짜 범위
    start = datetime(2018,12,1)
    end = datetime.now()

    if start > end:
        print("시작날짜와 끝 날짜를 다시 확인해주세요")
        quit(1)


    headers = { # 이 부분은 임의로 수정하지 마세요
        "authority": "finance.naver.com",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36", 
        "referer": "finance.naver.com"
        }
    params = {
        "code": "", #종목코드
        "page": 1 # 페이지
        }
    

    # stockName에 있는 종목명들을 토대로
    for sn in stockNames:
        print(sn + " 시작")
        #articleList에 들어가는 데이터는 (링크, 작성날짜)
        articleList = pd.DataFrame(columns=["URL","date"])
        #종목 코드를 구하고
        sc = stockDF.loc[stockDF["회사명"] == sn, "종목코드"].values[0]
        params["code"] = str(f"{sc:06}")
        print(sn, params["code"], "불러오는 중....")
        
        #페이지를 넘어다니며 글 목록을 불러옴
        p = 0#현재 페이지
        breakNow=False
        while True:
            if breakNow: break
            p+=1
            params["page"] = p
            response = requests.get("https://finance.naver.com/item/board.nhn", params=params, headers=headers)
            soup = BeautifulSoup(response.content, "html.parser")
            
            try:
                bbsList = soup.select("#content > div.section.inner_sub > table.type2 > tbody > tr > td.title > a") # 게시판 글 목록
                bbsDate = soup.select("#content > div.section.inner_sub > table.type2 > tbody > tr > td > span") # 작성 날짜
                print("감지된 글",len(bbsList), end='\t')
                #그리고 저장함
                for i in range(0, len(bbsList)):
                    writtenDate = bbsDate[2*i].text.split()[0].split(".")
                    writtenDate = datetime(int(writtenDate[0]), int(writtenDate[1]), int(writtenDate[2]))
                    # 작성 날짜가 대상 범위를 벗어나면
                    if start > writtenDate or end < writtenDate: 
                        raise AssertionError(f"{p} 페이지 - 지정한 날짜 범위를 벗어남 {writtenDate}") 

                    if len(articleList.loc[(articleList["URL"] == "https://finance.naver.com" + bbsList[i]["href"])]) >= 1:
                        breakNow=True
                        print("페이지의 끝에 도달")
                        break
                    else:
                        articleList = articleList.append({
                            'URL': "https://finance.naver.com" + bbsList[i]["href"], 
                            'date': writtenDate,
                        }, ignore_index=True)
                        
                print(p, "페이지 완료", "누적:", len(articleList))
            except AssertionError as ae:
                print(ae)
                #만약 시작 날짜 이전의 게시글만 있다면
                if start > writtenDate:
                    print("시작 날짜 이전의 게시글만 있어서 조기 종료")
                    break #조기종료
            except Exception as e:
                print("글 목록을 불러오는데 실패했습니다:", e)

        #작업 배분
        print(sn ,"글의 개수:",len(articleList))
        cpuCount = multiprocessing.cpu_count()
        td = end - start
        perCore = td.days/cpuCount
        #perCore = 7
        taskList=[]
        start_ = start
        end_ = start + timedelta(days=perCore)

        print("크롤링 시작")

        while start_ < end:
            if end_ > datetime.now(): end_ = datetime.now()
            taskList.append(articleList.loc[(start_ <= articleList["date"]) & (articleList["date"] < end_)])
            print(start_, end_)
            start_+=timedelta(days=perCore)
            end_+=timedelta(days=perCore)
        else:
            print(start_)
            taskList.append(articleList.loc[(start_ <= articleList["date"])])

        # 자식 프로세스를 만들고 실행
        process_list=[]
        #queues_list=[]
        splited_result=[]
        saveDir = "종토방/" + sn
        if not os.path.exists(saveDir): os.makedirs(saveDir)
        for dataFrame in taskList:
            #queue = multiprocessing.Queue()
            proc = multiprocessing.Process(target=download, args=(dataFrame,saveDir,sn,None, ), daemon=True)
            proc.start()
            process_list.append(proc)
            #queues_list.append(queue)

        """
        print("결과 취합중")
        for i, t in enumerate(process_list):
            splited_result.append(queues_list[i].get())
        """
        for t in process_list:
            t.join()

        #column과 total result를 만들기 위함
        """
        for df in splited_result:
            if df is not None:
                total_result = pd.DataFrame(None, columns=df.columns)
                break
        for df in splited_result:
            total_result = total_result.append(df, ignore_index=True)

        print(total_result)
        print("취합 완료, 크롤링된 개수:", len(total_result))
        total_result.to_pickle(os.path.join(saveDir, f"{sn} {start.year}-{start.month}-{start.day} to {end.year}-{end.month}-{end.day}.pkl"))
        """

        print(sn + " 완료")

        sleep(10)
    