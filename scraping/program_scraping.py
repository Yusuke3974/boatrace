####
#番組表のスクレイピングを行うプログラム#
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
SAVE_DIR = "./data/program_lzhs/"
# 解凍したファイルを保存する場所を指定
TXT_FILE_DIR = "./data/program_results_txt/"
# CSVファイルを保存する場所を指定
CSV_FILE_DIR = "./data/program_results_csv/"
# CSVファイルの名前を指定
CSV_FILE_NAME = "program_results.csv"

CSV_DETAILS_DIR = "./data/program_details_csv/"
CSV_DETAILS_NAME = "timetable_YYYYMMDD-YYYYMMDD.csv"


# リクエスト間隔を指定(秒)　※サーバに負荷をかけないよう3秒以上を推奨
INTERVAL = 3

CSV_FILE_HEADER = "タイトル,日次,レース日,レース場,レース回,レース名,距離(m),電話投票締切予定,\
1枠_艇番,1枠_登録番号,1枠_選手名,1枠_年齢,1枠_支部,1枠_体重,1枠_級別,\
1枠_全国勝率,1枠_全国2連対率,1枠_当地勝率,1枠_当地2連対率,\
1枠_モーター番号,1枠_モーター2連対率,1枠_ボート番号,1枠_ボート2連対率,\
1枠_今節成績_1-1,1枠_今節成績_1-2,1枠_今節成績_2-1,1枠_今節成績_2-2,1枠_今節成績_3-1,1枠_今節成績_3-2,\
1枠_今節成績_4-1,1枠_今節成績_4-2,1枠_今節成績_5-1,1枠_今節成績_5-2,1枠_今節成績_6-1,1枠_今節成績_6-2,1枠_早見,\
2枠_艇番,2枠_登録番号,2枠_選手名,2枠_年齢,2枠_支部,2枠_体重,2枠_級別,\
2枠_全国勝率,2枠_全国2連対率,2枠_当地勝率,2枠_当地2連対率,\
2枠_モーター番号,2枠_モーター2連対率,2枠_ボート番号,2枠_ボート2連対率,\
2枠_今節成績_1-1,2枠_今節成績_1-2,2枠_今節成績_2-1,2枠_今節成績_2-2,2枠_今節成績_3-1,2枠_今節成績_3-2,\
2枠_今節成績_4-1,2枠_今節成績_4-2,2枠_今節成績_5-1,2枠_今節成績_5-2,2枠_今節成績_6-1,2枠_今節成績_6-2,2枠_早見,\
3枠_艇番,3枠_登録番号,3枠_選手名,3枠_年齢,3枠_支部,3枠_体重,3枠_級別,\
3枠_全国勝率,3枠_全国2連対率,3枠_当地勝率,3枠_当地2連対率,\
3枠_モーター番号,3枠_モーター2連対率,3枠_ボート番号,3枠_ボート2連対率,\
3枠_今節成績_1-1,3枠_今節成績_1-2,3枠_今節成績_2-1,3枠_今節成績_2-2,3枠_今節成績_3-1,3枠_今節成績_3-2,\
3枠_今節成績_4-1,3枠_今節成績_4-2,3枠_今節成績_5-1,3枠_今節成績_5-2,3枠_今節成績_6-1,3枠_今節成績_6-2,3枠_早見,\
4枠_艇番,4枠_登録番号,4枠_選手名,4枠_年齢,4枠_支部,4枠_体重,4枠_級別,\
4枠_全国勝率,4枠_全国2連対率,4枠_当地勝率,4枠_当地2連対率,\
4枠_モーター番号,4枠_モーター2連対率,4枠_ボート番号,4枠_ボート2連対率,\
4枠_今節成績_1-1,4枠_今節成績_1-2,4枠_今節成績_2-1,4枠_今節成績_2-2,4枠_今節成績_3-1,4枠_今節成績_3-2,\
4枠_今節成績_4-1,4枠_今節成績_4-2,4枠_今節成績_5-1,4枠_今節成績_5-2,4枠_今節成績_6-1,4枠_今節成績_6-2,4枠_早見,\
5枠_艇番,5枠_登録番号,5枠_選手名,5枠_年齢,5枠_支部,5枠_体重,5枠_級別,\
5枠_全国勝率,5枠_全国2連対率,5枠_当地勝率,5枠_当地2連対率,\
5枠_モーター番号,5枠_モーター2連対率,5枠_ボート番号,5枠_ボート2連対率,\
5枠_今節成績_1-1,5枠_今節成績_1-2,5枠_今節成績_2-1,5枠_今節成績_2-2,5枠_今節成績_3-1,5枠_今節成績_3-2,\
5枠_今節成績_4-1,5枠_今節成績_4-2,5枠_今節成績_5-1,5枠_今節成績_5-2,5枠_今節成績_6-1,5枠_今節成績_6-2,5枠_早見,\
6枠_艇番,6枠_登録番号,6枠_選手名,6枠_年齢,6枠_支部,6枠_体重,6枠_級別,\
6枠_全国勝率,6枠_全国2連対率,6枠_当地勝率,6枠_当地2連対率,\
6枠_モーター番号,6枠_モーター2連対率,6枠_ボート番号,6枠_ボート2連対率,\
6枠_今節成績_1-1,6枠_今節成績_1-2,6枠_今節成績_2-1,6枠_今節成績_2-2,6枠_今節成績_3-1,6枠_今節成績_3-2,\
6枠_今節成績_4-1,6枠_今節成績_4-2,6枠_今節成績_5-1,6枠_今節成績_5-2,6枠_今節成績_6-1,6枠_今節成績_6-2,6枠_早見\n"

# CSVファイルのヘッダーを指定


# URLの固定部分を指定
FIXED_URL = "http://www1.mbrace.or.jp/od2/B/"

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

        variable_url = FIXED_URL + yyyymm + "/b" + yymmdd + ".lzh"
        file_name = "b" + yymmdd + ".lzh"
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
            trans_asc = str.maketrans('１２３４５６７８９０Ｒ：　', '1234567890R: ')
            if re.search(r"番組表", contents):
                # 1行スキップ
                text_file.readline()

                # タイトルを格納
                line = text_file.readline()
                title = line[:-1].strip()

                # 1行スキップ
                text_file.readline()

                # 日次・レース日・レース場を格納
                line = text_file.readline()
                day = line[3:7]
                date = line[17:28]
                stadium = line[52:55]

            # キーワード「電話投票締切予定」を見つけたら
            if re.search(r"電話投票締切予定", contents):

                # キーワードを見つけた行を格納
                line = contents

            # レース名にキーワード「進入固定」が割り込んだ際の補正(「進入固定戦隊」は除くためＨまで含めて置換)
                if re.search(r"進入固定", line):
                    line = line.replace('進入固定 Ｈ', '進入固定     Ｈ')

                # レース回・レース名・距離(m)・電話投票締切予定を格納
                race_round = line[0:3].translate(trans_asc).replace(' ', '0')
                race_name = line[5:21].replace('　', '')
                distance = line[22:26].translate(trans_asc)
                post_time = line[37:42].translate(trans_asc)

                # 4行スキップ(ヘッダー部分)
                text_file.readline()
                text_file.readline()
                text_file.readline()
                text_file.readline()

                # 選手データを格納する変数を定義
                racer_data = ""

                # 選手データを読み込む行(開始行)を格納
                line = text_file.readline()

                # 空行またはキーワード「END」まで処理を繰り返す = 1～6艇分の選手データを取得
                while line != "\n":
                    if re.search(r"END", line):
                        break

                    # 選手データを格納(行末にカンマが入らないように先頭にカンマを入れる)
                    racer_data += "," + line[0] + "," + line[2:6] + "," + line[6:10] + "," + line[10:12] \
                                + "," + line[12:14] + "," + line[14:16] + "," + line[16:18] \
                                + "," + line[19:23] + "," + line[24:29] + "," + line[30:34] \
                                + "," + line[35:40] + "," + line[41:43] + "," + line[44:49] \
                                + "," + line[50:52] + "," + line[53:58] + "," + line[59:60] \
                                + "," + line[60:61] + "," + line[61:62] + "," + line[62:63] \
                                + "," + line[63:64] + "," + line[64:65] + "," + line[65:66] \
                                + "," + line[66:67] + "," + line[67:68] + "," + line[68:69] \
                                + "," + line[69:70] + "," + line[70:71] + "," + line[71:73]

                    # 次の行を読み込む
                    line = text_file.readline()
                            # CSV に書き込み
                with open(CSV_FILE_DIR + CSV_FILE_NAME, "a", encoding="utf-8") as csv_file:
                        csv_file.write(title + "," + day + "," + date + "," + stadium + "," + race_round
                            + "," + race_name + "," + distance + "," + post_time + racer_data + "\n")

    

def create_csv():
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