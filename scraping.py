from time import sleep
from requests import get
from datetime import datetime as dt
from datetime import timedelta as td
from os import makedirs
import re
import lhafile
import os
import chardet


# 開始日と終了日を指定(YYYY-MM-DD)
START_DATE = "2021-09-01"
END_DATE = "2021-09-02"

# ファイルの保存先を指定　※コラボでGoogleドライブをマウントした状態を想定
SAVE_DIR = "./data/lzhs/"
# 解凍したファイルを保存する場所を指定
TXT_FILE_DIR = "./data/results_txt/"
# CSVファイルを保存する場所を指定
CSV_FILE_DIR = "./data/results_csv/"

# CSVファイルの名前を指定
CSV_FILE_NAME = "results.csv"

# リクエスト間隔を指定(秒)　※サーバに負荷をかけないよう3秒以上を推奨
INTERVAL = 3

# URLの固定部分を指定
FIXED_URL = "http://www1.mbrace.or.jp/od2/K/"

def scraping():
    print("LDHファイル取得を開始します")
    makedirs(SAVE_DIR, exist_ok=True)

    # 開始日と終了日を日付型に変換して格納
    start_date = dt.strptime(START_DATE, '%Y-%m-%d')
    end_date = dt.strptime(END_DATE, '%Y-%m-%d')

    # 日付の差から期間を計算
    days_num = (end_date - start_date).days + 1

    # 日付リストを格納する変数
    date_list = []

    # 日付リストを生成
    for i in range(days_num):
        target_date = start_date + td(days=i)

        # 日付型を文字列に変換してリストに格納(YYYYMMDD)
        date_list.append(target_date.strftime("%Y%m%d"))

    # URL生成とダウンロード
    for date in date_list:
        yyyymm = date[0:4] + date[4:6]
        yymmdd = date[2:4] + date[4:6] + date[6:8]

        variable_url = FIXED_URL + yyyymm + "/k" + yymmdd + ".lzh"
        file_name = "k" + yymmdd + ".lzh"
        r = get(variable_url)

        # 成功した場合
        if r.status_code == 200:
            f = open(SAVE_DIR + file_name, 'wb')
            f.write(r.content)
            f.close()
            print(variable_url + " をダウンロードしました")

        # 失敗した場合
        else:
            print(variable_url + " のダウンロードに失敗しました")

        # 指定した間隔をあける
        sleep(INTERVAL)

    print("作業を終了しました")
    

def decompress():
    # 開始合図
    print("LDH解凍とTXT保存を開始します")

    # ファイルを格納するフォルダを作成
    os.makedirs(TXT_FILE_DIR, exist_ok=True)

    # LZHファイルのリストを取得
    lzh_file_list = os.listdir(SAVE_DIR)

    # ファイルの数だけ処理を繰り返す
    for lzh_file_name in lzh_file_list:

        # 拡張子が lzh のファイルに対してのみ実行
        if re.search(".lzh", lzh_file_name):

            file = lhafile.Lhafile(SAVE_DIR + lzh_file_name)

            # 解凍したファイルの名前を取得
            info = file.infolist()
            name = info[0].filename

            # 解凍したファイルの保存
            open(TXT_FILE_DIR + name, "wb").write(file.read(name))

            print(TXT_FILE_DIR + lzh_file_name + " を解凍しました")

    # 終了合図
    print("作業を終了しました")

def detect_encoding(file_path):
    """ファイルのエンコーディングを判定する"""
    with open(file_path, 'rb') as f:
        raw_data = f.read()
    result = chardet.detect(raw_data)
    return result['encoding']

# テキストファイルからデータを抽出し、CSVファイルに書き込む関数
def get_data(text_file_path):
    """エンコーディングを自動判定してファイルを開く"""
    encoding = detect_encoding(text_file_path)
    print(f"Detected encoding for {text_file_path}: {encoding}")

    with open(text_file_path, "r", encoding=encoding) as text_file:
        for contents in text_file:
            if re.search(r"競走成績", contents):
                text_file.readline()
                title = text_file.readline().strip()
                text_file.readline()
                line = text_file.readline()
                day = line[3:7].replace(' ', '')
                date = line[17:27].replace(' ', '')
                stadium = line[62:65].replace('　', '')

            if re.search(r"払戻金", contents):
                line = text_file.readline()
                while line != "\n":
                    results = line[10:13].strip() + "," \
                              + line[15:20] + "," + line[21:28].strip() + "," \
                              + line[32:37] + "," + line[38:45].strip() + "," \
                              + line[49:52] + "," + line[53:60].strip() + "," \
                              + line[64:67] + "," + line[68:75].strip()
                    
                    # CSV に書き込み
                    with open(CSV_FILE_DIR + CSV_FILE_NAME, "a", encoding="utf-8") as csv_file:
                        csv_file.write(title + "," + day + "," + date + "," + stadium + "," + results + "\n")
                    
                    line = text_file.readline()


def create_csv():
    CSV_FILE_HEADER = "タイトル,日次,レース日,レース場,レース回,\
    3連単_組番,3連単_払戻金,3連複_組番,3連複_払戻金,2連単_組番,2連単_払戻金,2連複_組番,2連複_払戻金\n"

    print("CSV作成作業を開始します")

    os.makedirs(CSV_FILE_DIR, exist_ok=True)

    # CSVファイルを作成しヘッダ情報を書き込む
    with open(CSV_FILE_DIR + CSV_FILE_NAME, "w", encoding="utf-8") as csv_file:
        csv_file.write(CSV_FILE_HEADER)

    # `.TXT` ファイルのリストを取得
    text_file_list = os.listdir(TXT_FILE_DIR)

    for txt_file_name in text_file_list:
        if re.search(r"\.TXT$", txt_file_name):  # `.TXT` のみ処理
            text_file_path = os.path.join(TXT_FILE_DIR, txt_file_name)
            get_data(text_file_path)  # ファイルパスを渡す

    print(CSV_FILE_DIR + CSV_FILE_NAME + " を作成しました")
    print("作業を終了しました")


def main():
    scraping()
    decompress()
    create_csv()
    
if __name__ == '__main__':
    main()