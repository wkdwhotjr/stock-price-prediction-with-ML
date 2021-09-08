import pandas as pd
import pickle
import keras.applications, keras.datasets, keras.preprocessing, keras.wrappers
from keras_bert import Tokenizer
import os
import re
import numpy as np
from tensorflow.keras.models import load_model
import codecs
import argparse

parser = argparse.ArgumentParser('종토방 긍부정 예측')
parser.add_argument('--model', '-m', help = '모델 경로 (.h5)', type = str, required = True)
args = parser.parse_args()
model_path = args.model

SEQ_LEN=32
sent_model = load_model(model_path)

pretrained_path ="bert_eojeol_tensorflow"
vocab_path = os.path.join(pretrained_path, 'vocab.korean.rawtext.list')
token_dict = {}

with codecs.open(vocab_path, 'r', 'utf8') as reader:
    for line in reader:
        token = line.strip().split('\t')[0]
        if "_" in token:
          token = token.replace("_","")
          token = "##" + token
        token_dict[token] = len(token_dict)

class inherit_Tokenizer(Tokenizer):
  def _tokenize(self, text):
        if not self._cased:
            text = text
            
            text = text.lower()
        spaced = ''
        for ch in text:
            if self._is_punctuation(ch) or self._is_cjk_character(ch):
                spaced += ' ' + ch + ' '
            elif self._is_space(ch):
                spaced += ' '
            elif ord(ch) == 0 or ord(ch) == 0xfffd or self._is_control(ch):
                continue
            else:
                spaced += ch
        tokens = []
        for word in spaced.strip().split():
            tokens += self._word_piece_tokenize(word)
        return tokens

tokenizer = inherit_Tokenizer(token_dict)


def read_dir(path):
    total_df = pd.DataFrame(None, columns=["stock_name","date","title","content"])
    for f in os.listdir(os.path.join("종토방", path)):
        if not f.endswith(".csv"): continue
        try:
            read = pd.read_csv(f"종토방/{path}/{f}", index_col=0, encoding="cp949")
        #인코딩 문제가 있다면?
        #if not all(read.stock_name==dir):
        except UnicodeDecodeError:
            read = pd.read_csv(f"종토방/{path}/{f}", index_col=0, encoding="utf8")

        total_df = pd.concat([total_df, read])
    total_df = total_df.sort_index()
    total_df.dropna(inplace=True)
    total_df = total_df.drop_duplicates(['title','content'])
    return total_df

def sentence_convert_data(data):
    global tokenizer
    indices = []
    for i in range(len(data)):
        raw = data.iloc[i]

        #데이터 클렌징
        pattern1 = re.compile("\(.+?\)") #괄호 제거
        pattern2 = re.compile("[^(가-힣a-zA-Z \?\r\.,)]")#특수문자 및 숫자 제거
        cleaned = pattern1.sub("", raw)
        cleaned = pattern2.sub("", cleaned)

        ids, segments = tokenizer.encode(cleaned, max_len=SEQ_LEN)
        indices.append(ids)
        
    items = indices
    indices = np.array(indices)
    return indices

def sentence_load_data(sentences):#sentence는 List로 받는다
           
    data_x = sentence_convert_data(sentences)

    return data_x

file_ls = os.listdir("종토방")
company_names = ["STX엔진"]
    
for cn in company_names:
    if not cn in file_ls:
        print(cn, "이 목록에 없습니다")
        quit(1)

for idx, company_name in enumerate(company_names):
    df = read_dir(company_name)

    try:
        df["document"] = df["title"] + "\r" + df["content"]
        X = sentence_load_data(df["document"])
        
        df["label"] = sent_model.predict(X)
        #df.drop(["document"], axis=1, inplace=True)

        df.to_csv(f"종토방_긍부정평가결과/{company_name}.csv", encoding="utf8")
        print(company_name, "완료", f"{idx+1}/{len(company_names)}")
    
    except Exception as e:
        print(company_name,"하던 도중 오류 발생",e, type(e))