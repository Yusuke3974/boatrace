####
#出戻り金のスクレイピングを行うプログラム#
####

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
SAVE_DIR = "./data/return_lzhs/"
# 解凍したファイルを保存する場所を指定
TXT_FILE_DIR = "./data/return_results_txt/"
# CSVファイルを保存する場所を指定
CSV_FILE_DIR = "./data/return_results_csv/"

# CSVファイルの名前を指定
CSV_FILE_NAME = "return_results.csv"

CSV_DETAILS_DIR = "./data/race_details_csv/"
CSV_DETAILS_NAME = "results_YYYYMMDD-YYYYMMDD.csv"

# リクエスト間隔を指定(秒)　※サーバに負荷をかけないよう3秒以上を推奨
INTERVAL = 3

# URLの固定部分を指定
FIXED_URL = "http://www1.mbrace.or.jp/od2/K/"

CSV_FILE_HEADER_DETAILS = "レースコード,タイトル,日次,レース日,レース場,\
レース回,レース名,距離(m),天候,風向,風速(m),波の高さ(cm),決まり手,\
単勝_艇番,単勝_払戻金,複勝_1着_艇番,複勝_1着_払戻金,複勝_2着_艇番,複勝_2着_払戻金,\
2連単_組番,2連単_払戻金,2連単_人気,2連複_組番,2連複_払戻金,2連複_人気,\
拡連複_1-2着_組番,拡連複_1-2着_払戻金,拡連複_1-2着_人気,\
拡連複_1-3着_組番,拡連複_1-3着_払戻金,拡連複_1-3着_人気,\
拡連複_2-3着_組番,拡連複_2-3着_払戻金,拡連複_2-3着_人気,\
3連単_組番,3連単_払戻金,3連単_人気,3連複_組番,3連複_払戻金,3連複_人気,\
1着_着順,1着_艇番,1着_登録番号,1着_選手名,1着_モーター番号,1着_ボート番号,\
1着_展示タイム,1着_進入コース,1着_スタートタイミング,1着_レースタイム,\
2着_着順,2着_艇番,2着_登録番号,2着_選手名,2着_モーター番号,2着_ボート番号,\
2着_展示タイム,2着_進入コース,2着_スタートタイミング,2着_レースタイム,\
3着_着順,3着_艇番,3着_登録番号,3着_選手名,3着_モーター番号,3着_ボート番号,\
3着_展示タイム,3着_進入コース,3着_スタートタイミング,3着_レースタイム,\
4着_着順,4着_艇番,4着_登録番号,4着_選手名,4着_モーター番号,4着_ボート番号,\
4着_展示タイム,4着_進入コース,4着_スタートタイミング,4着_レースタイム,\
5着_着順,5着_艇番,5着_登録番号,5着_選手名,5着_モーター番号,5着_ボート番号,\
5着_展示タイム,5着_進入コース,5着_スタートタイミング,5着_レースタイム,\
6着_着順,6着_艇番,6着_登録番号,6着_選手名,6着_モーター番号,6着_ボート番号,\
6着_展示タイム,6着_進入コース,6着_スタートタイミング,6着_レースタイム,\n"

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

def get_data_race_detail(text_file_path):
    """エンコーディングを自動判定してファイルを開く"""
    encoding = detect_encoding(text_file_path)
    print(f"Detected encoding for {text_file_path}: {encoding}")
    
    with open(text_file_path, "r", encoding=encoding) as text_file:
        for line in text_file:
            # キーワード「競争成績」を見つけたら(rは正規表現でraw文字列を指定するおまじない)
            if re.search(r"競走成績", line):
                # 1行スキップ
                text_file.readline()

                # タイトルを格納
                line = text_file.readline()
                title = line[:-1].strip()
                # 1行スキップ
                text_file.readline()

                # 日次・レース日・レース場を格納
                line = text_file.readline()
                day = line[3:7].replace(' ', '')
                date = line[17:27].replace(' ', '0')
                stadium = line[62:65].replace('　', '')

            # レース回の「R」と距離の「H」を同じ行に見つけたら -> これ以降に競走成績の詳細が記載
            if re.search(r"R", line) and re.search(r"H", line):

                # レース名にキーワード「進入固定」が割り込んだ際の補正(「進入固定戦隊」は除くためＨまで含めて置換)
                if re.search(r"進入固定", line):
                    line = line.replace('進入固定       H', '進入固定           H')

                # レース回、レース名、距離(m)、天候、風向、風速(m)、波の高さ(cm)を取得
                race_round = line[2:5].replace(' ', '0')
                race_name = line[12:31].replace('　', '')
                distance = line[36:40]
                weather = line[43:45].strip()
                wind_direction = line[50:52].strip()
                wind_velocity = line[53:55].strip()
                wave_height = line[60:63].strip()

                # 決まり手を取得
                line = text_file.readline()
                winning_technique = line[50:55].strip()

                # 1行スキップ
                text_file.readline()

                # 選手データを格納する変数を定義
                result_racer = ""

                # 選手データを取り出す行(開始行)を格納
                line = text_file.readline()

                # 空行まで処理を繰り返す = 1～6艇分の選手データを取得
                while line != "\n":
                    # 選手データを格納(行末にカンマが入らないように先頭にカンマを入れる)
                    result_racer += "," + line[2:4] + "," + line[6] + "," + line[8:12] \
                                    + "," + line[13:21] + "," + line[22:24] + "," + line[27:29] \
                                    + "," + line[30:35].strip() + "," + line[38] + "," + line[43:47] \
                                    + "," + line[52:58]

                    # 次の行を読み込む
                    line = text_file.readline()

                # レース結果を取り出す行(開始行)を格納
                line = text_file.readline()

                # 空行まで処理を繰り返す = レース結果を取得
                while line != "\n":

                    # 単勝の結果を取得
                    if re.search(r"単勝", line):

                        # 文字列「特払い」が割り込んだ際の補正
                        if re.search(r"特払い", line):
                            line = line.replace('        特払い   ', '   特払い        ')

                        result_win = line[15] + "," + line[22:29].strip()

                    # 複勝の結果を取得
                    if re.search(r"複勝", line):

                        # 文字列「特払い」が割り込んだ際の補正
                        if re.search(r"特払い", line):
                            line = line.replace('        特払い   ', '   特払い        ')

                        # 複勝_2着のデータが存在しない場合の分岐
                        if len(line) <= 33:
                            result_place_show = line[15] + "," + line[22:29].strip() \
                                                + "," + ","
                        else:
                            result_place_show = line[15] + "," + line[22:29].strip() \
                                                + "," + line[31] + "," + line[38:45].strip()

                    # 2連単の結果を取得
                    if re.search(r"２連単", line):
                        result_exacta = line[14:17] + "," + line[21:28].strip() \
                                        + "," + line[36:38].strip()

                    # 2連複の結果を取得
                    if re.search(r"２連複", line):
                        result_quinella = line[14:17] + "," + line[21:28].strip() \
                                        + "," + line[36:38].strip()

                    # 拡連複の結果を取得
                    if re.search(r"拡連複", line):
                        # 1-2着
                        result_quinella_place = line[14:17] + "," + line[21:28].strip() \
                                                + "," + line[36:38].strip()

                        # 1-3着
                        line = text_file.readline()
                        result_quinella_place += "," + line[17:20] + "," + line[24:31].strip() \
                                                + "," + line[39:41].strip()

                        # 2-3着
                        line = text_file.readline()
                        result_quinella_place += "," + line[17:20] + "," + line[24:31].strip() \
                                                + "," + line[39:41].strip()

                    # 3連単の結果を取得
                    if re.search(r"３連単", line):
                        result_trifecta = line[14:19] + "," + line[21:28].strip() \
                                        + "," + line[35:38].strip()

                    # 3連複の結果を取得
                    if re.search(r"３連複", line):
                        result_trio = line[14:19] + "," + line[21:28].strip() \
                                    + "," + line[35:38].strip()

                    # 次の行を読み込む
                    line = text_file.readline()

                # レースコードを生成
                dict_stadium = {'桐生': 'KRY', '戸田': 'TDA', '江戸川': 'EDG', '平和島': 'HWJ',
                                '多摩川': 'TMG', '浜名湖': 'HMN', '蒲郡': 'GMG', '常滑': 'TKN',
                                '津': 'TSU', '三国': 'MKN', '琵琶湖': 'BWK', '住之江': 'SME',
                                '尼崎': 'AMG', '鳴門': 'NRT', '丸亀': 'MRG', '児島': 'KJM',
                                '宮島': 'MYJ', '徳山': 'TKY', '下関': 'SMS', '若松': 'WKM',
                                '芦屋': 'ASY', '福岡': 'FKO', '唐津': 'KRT', '大村': 'OMR',"びわこ": "BWK"}

                race_code = date[0:4] + date[5:7] + date[8:10] + dict_stadium[stadium] + race_round[0:2]
                with open(CSV_DETAILS_DIR + CSV_DETAILS_NAME,"a", encoding="utf-8") as csv_file:
                            csv_file.write(race_code + "," + title + "," + day + "," + date + "," + stadium \
                           + "," + race_round + "," + race_name + "," + distance + "," + weather \
                           + "," + wind_direction + "," + wind_velocity + "," + wave_height \
                           + "," + winning_technique + "," + result_win + "," + result_place_show \
                           + "," + result_exacta + "," + result_quinella + "," + result_quinella_place \
                           + "," + result_trifecta + "," + result_trio + result_racer + "\n")

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

def create_race_detail_csv():
    print("レース詳細CSV作成作業を開始します")
    os.makedirs(CSV_DETAILS_DIR, exist_ok=True)

    with open(CSV_DETAILS_DIR + CSV_DETAILS_NAME, "w", encoding="utf-8") as csv_file:
        csv_file.write(CSV_FILE_HEADER_DETAILS)

    # `.TXT` ファイルのリストを取得
    text_file_list = os.listdir(TXT_FILE_DIR)

    for txt_file_name in text_file_list:
        if re.search(r"\.TXT$", txt_file_name):  # `.TXT` のみ処理
            text_file_path = os.path.join(TXT_FILE_DIR, txt_file_name)
            print(f"Processing file: {text_file_path}")  # 追加
            get_data_race_detail(text_file_path)  # ファイルパスを渡す

    print(f"{CSV_DETAILS_DIR + CSV_DETAILS_NAME} を作成しました")
    print("作業を終了しました")

def main():
    scraping()
    decompress()
    create_csv()
    create_race_detail_csv()
    
if __name__ == '__main__':
    main()