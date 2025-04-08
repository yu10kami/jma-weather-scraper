#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
気象庁のサイトから過去の気象データをスクレイピングするプログラム

【制約条件】
* サーバ負荷を避けるため、各リクエスト間に 1～3 秒の待機時間を設定
* 対象地域：東京（東京都）
* 2009年1月1日以降のデータを対象（現在ある全てのデータ）
* データは10分ごとの値を取得
* CSV のカラムは下記の通り
    日付, 時間, 気圧(現地), 気圧(海面), 降水量(mm), 気温, 相対湿度, 
    風向・風速(平均), 風向・風速(風向), 風向・風速(最大瞬間), 風向・風速(風向), 日照時間
* 各年各月ごとにCSV出力（ファイル名はYYYYMM.csv）
"""

import requests
import time
import random
import calendar
import pandas as pd
from bs4 import BeautifulSoup
import datetime


def fetch_data(year, month, day):
    url = f"https://www.data.jma.go.jp/stats/etrn/view/10min_s1.php?prec_no=44&block_no=47662&year={year}&month={month:02d}&day={day:02d}"
    print(f"リクエストURL: {url}")

    # サーバ負荷軽減のため、1～3秒のランダムな待機
    time.sleep(random.uniform(1, 3))
    
    response = requests.get(url)
    if response.status_code != 200:
        print(f"{year}-{month:02d}--{day:02d} のデータ取得に失敗しました。ステータスコード: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # ※ テーブルのクラス名等はサイト仕様に合わせて調整してください
    table = soup.find("table", attrs={"class": "data2_s"})
    if table is None:
        print(f"{year}-{month:02d}--{day:02d} のデータテーブルが見つかりませんでした。")
        return None

    rows = table.find_all("tr")
    data = []
    for row in rows:
        cols = row.find_all(["td", "th"])
        # 各セルのテキストを取得
        cols_text = [col.get_text(strip=True) for col in cols]
        # ヘッダー行や空行はスキップ
        if not cols_text or "時分" in cols_text[0]:
            continue
        # 必要なカラム数（例では12カラム以上）がある行のみを対象とする
        if len(cols_text) >= 11:
            # 指定のカラム数（先頭12列）を取得
            cols_text.insert(0, f"{year}{month:02d}{day:02d}")
            data.append(cols_text[:12])

    # DataFrame に変換（カラム名は制約条件に基づく）
    df = pd.DataFrame(data, columns=[
        "日付", "時間", "気圧(現地)", "気圧(海面)", "降水量(mm)",
        "気温", "相対湿度", "風向・風速(平均)", "風向・風速(風向-平均)",
        "風向・風速(最大瞬間)", "風向・風速(風向-最大瞬間)", "日照時間"
    ])
    return df

def main():
    # 2009年1月1日から現在までの年月日を設定
    start_year = 2009
    start_month = 1
    start_day = 1
    today = datetime.date.today()
    end_year = today.year
    end_month = today.month
    end_day = today.day

    for year in range(start_year, end_year + 1):
        # 対象年の開始月と終了月を決定
        month_start = 1 if year > start_year else start_month
        month_end = 12 if year < end_year else end_month
        for month in range(month_start, month_end + 1):
            # 当該月の日数を取得
            last_day = calendar.monthrange(year, month)[1]
            # 現在月の場合、今日までとする
            if year == end_year and month == end_month:
                day_end = end_day
            else:
                day_end = last_day

            monthly_data = []
            for day in range(1, day_end + 1):
                print(f"{year}-{month:02d}-{day:02d} のデータを取得中...")
                df_day = fetch_data(year, month, day)
                if df_day is not None and not df_day.empty:
                    monthly_data.append(df_day)
                else:
                    print(f"{year}-{month:02d}-{day:02d} のデータが存在しないか、取得できませんでした。")
            
            if monthly_data:
                # 日毎のデータを結合して、月単位の DataFrame を作成
                df_month = pd.concat(monthly_data, ignore_index=True)
                filename = f"{year}{month:02d}.csv"
                df_month.to_csv(filename, index=False, encoding="utf-8-sig")
                print(f"CSVファイル {filename} に保存しました。")
            else:
                print(f"{year}-{month:02d} のデータが全て取得できませんでした。")

if __name__ == "__main__":
    main()