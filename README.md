# 정형/비정형 데이터 활용 저평가 우량주 예측


## 사용 기술
- 사용 언어 : Python, JavaScript
- 개발 툴 : Jupyter Notebook, VScode
- API : Open Dart API
- 운영체제 : Windows 10
- 라이브러리 : Numpy, Pandas, Scikit-Learn, LightGBM, TensorFlow, Keras, BS4, Selenium, pykrx, TA-Lib, dart_fss, CanvasJS, ChartJS


## 코드 설명
![Project Flow Chart](https://user-images.githubusercontent.com/76696543/121051682-9d4dc780-c7f4-11eb-9b7e-7fb0b236c11a.png)


### 1. 저평가주 탐색 모듈
- 실행 코드 : find_undervalued_stock.ipynb
- 모듈 : undervaluedstock.py
```python
import undervaluedstock as uv
```
- parameter
  - last_business_day : (default = None) True일 시 가장 최근 영업일의 저평가주를 탐색하여 DataFrame으로 반환
  ```python
  undervalued_df_today = uv.find_undervalued_stock(last_business_day=True)
  ```
  - day : (default = None) [str] 입력 시 해당 날짜의 저평가주를 탐색하여 DataFrame으로 반환 (해당 날짜가 주말 및 공휴일일 시 가장 최근 영업일을 기준으로 반환)
  ```python
  undervalued_df = uv.find_undervalued_stock(day='20210101')

### 2. 정형 데이터 칼럼 생성
- 실행 코드 : Quantative_Analysis.ipynb
- 모듈 : QuantativeAnalysis.py
```python
import QuantativeAnalysis as qa
```
- Method
  - dictionary_update : {종목이름 : [시장, 종목코드, WICS산업분류]} 의 형태로 dictionary를 생성하여 pickle 파일로 저장
  ```python
  qa.dictionary_update()
  ```
  - get_stock_price_date : 주가 정보를 칼럼으로 지닌 DataFrame을 생성
    - start_date : [str], 관심 기간의 시작일
    - end_date : [str], 관심 기간의 종료일
    - stock_list : [list, array-like], 관심 종목의 이름들로 이루어진 list 및 array-like 
  ```python
  stock_list = ['삼성전자', 'NAVER', 'GS건설']
  df = qa.get_stock_price_data(start_date='20210101', end_date='20210131', stock_list=stock_list)
  ```
- Class - FinancialStatements : 재무비율 생성
  - path : [str], 재무제표 파일이 있는 디렉토리의 경로 입력
  ```python
  from QuantativeAnalysis import FinancialStatements
  
  fs_instance = FinancialStstements(path='./fsdata/')
  ```
  - Class method
    - get_fsr : 재무제표에서 항목들을 추출하여 재무 비율을 산출하고 종목코드.pickle의 형태로 저장 (pickle파일을 저장할 폴더로 fsr 자동 생성)
    ```python
    fs_instance.get_fsr()
    ```
    - mapping_fsr : 입력받은 DataFrame에 종목 이름, 날짜를 기준으로 재무비율을 join하여 DataFrame의 형태로 반환
    ```python
    fs_df = fs_instance.mapping_fsr(df)
    ```
- Class - TechnicalAnalysis : 기술적 분석에서 사용하는 보조지표 생성
  - df : [DataFrame] 보조지표들을 join할 DataFrame 입력
  ```python
  from QuantativeAnalysis import TechnicalAnalysis
  
  ta_instance = TechnicalAnalysis(df=fs_df)
  ```
  - Class method
    - get_TA : 보조지표들을 산출하여 Class 초기화시 입력된 DataFrame에 종목 이름과 날짜를 기준으로 join
    ```python
    ta_df = ta_instance.get_TA()
    ```

### 3. 비정형 데이터 칼럼 생성 : 뉴스 기사 분석
- 실행 코드 : 긍부정 평가 및 label, 분류모델.ipynb
- 사용한 사용자 함수
  - sentiment_predict : LSTM 모델을 사용한 뉴스 기사 제목 긍부정 점수 산출
  - positive_nagative_score : 단어사전을 사용한 뉴스 기사 내용 키워드 긍부정 점수 산출
  - new_columns : 1주차, 2주차, 3주차, 4주차 키워드분석, 제목분석, 기사갯수 카운트 변수 생성

### 4. 비정형 데이터 칼럼 생성 : 종목 토론방 게시글 분석
- 종목토론방 게시글 데이터 수집
  - 사용 모듈
  ```
  ./종토방크롤링/종토방 multiprocessing.py
  ``` 
- 종목토론방 게시글의 긍부정 예측을 위한 LSTM 모델 훈련
  - 데이터 훈련은 라벨링된 12,000개의 데이터를 기반으로 실시
  - 9,000개의 학습 데이터와 1,500개의 검증 데이터 및 1,500개의 테스트 데이터로 구성
  - label은 0(부정)과 1(긍정)으로 구성
  - 긍정도란 label이 1(긍정)일 확률을 의미
- LSTM 모델 활용 및 변수 창출
  - 실행 코드
  ```
  ./종토방크롤링/종토방_데이터셋제작.ipynb
  ```

### 5. 시각화 서비스
- 파일 경로
```
./DART_web/
```
    
    

