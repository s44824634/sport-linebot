import os
import random
import time
import re
import json
import requests
import pandas as pd
import threading
from io import StringIO
from functools import cached_property
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from flask import Flask, request, abort
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, TextMessage, PushMessageRequest
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent

app = Flask(__name__)

CHANNEL_SECRET = os.environ.get("CHANNEL_SECRET", "")
CHANNEL_ACCESS_TOKEN = os.environ.get("CHANNEL_ACCESS_TOKEN", "")

configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

user_agent = UserAgent()
back_links = ["https://www.juksy.com/article/98898", "", ""]

alliance_dict = {
    "NBA": "3", "MLB": "1", "足球": "4", "日本職棒": "2",
    "NHL冰球": "91", "美式足球": "93", "歐洲職籃": "8",
    "韓國職籃": "92", "中國職籃": "94", "日本職籃": "97", "澳洲職籃": "12",
}

NBA_team = ["底特律活塞","休士頓火箭","猶他爵士","明尼蘇達灰狼","達拉斯獨行俠","洛杉磯湖人","曼斐斯灰熊","紐約尼克","沙加緬度國王","芝加哥公牛","丹佛金塊","波士頓塞爾提克","費城76人","印第安那溜馬","紐奧良鵜鶘","鳳凰城太陽","金州勇士","布魯克林籃網","邁阿密熱火","密爾瓦基公鹿","洛杉磯快艇","亞特蘭大老鷹","奧克拉荷馬雷霆","聖安東尼奧馬刺","華盛頓巫師","多倫多暴龍","奧蘭多魔術","夏洛特黃蜂","波特蘭拓荒者","克里夫蘭騎士"]
MLB_team = ["亞特蘭大勇士","邁阿密馬林魚","紐約大都會","費城費城人","華盛頓國民","芝加哥小熊","辛辛那堤紅人","密爾瓦基釀酒人","匹茲堡海盜","聖路易紅雀","亞歷桑那響尾蛇","科羅拉多落磯","洛杉磯道奇","聖地牙哥教士","舊金山巨人","巴爾的摩金鶯","波士頓紅襪","紐約洋基","坦帕灣光芒","多倫多藍鳥","芝加哥白襪","克里夫蘭守護者","底特律老虎","堪薩斯皇家","明尼蘇達雙城","休士頓太空人","洛杉磯天使","奧克蘭運動家","西雅圖水手","德州遊騎兵"]
NPB_team = ["讀賣巨人","養樂多燕子","橫濱海灣之星","中日龍","阪神虎","廣島東洋鯉魚","日本火腿鬥士","樂天金鷹","西武獅","羅德海洋","歐力士猛牛","軟體銀行鷹"]
Korea_team = ["首爾三星迅雷","蔚山現代太陽神","釜山KCC宙斯盾","昌原LG獵隼","首爾SK騎士","高陽索諾天空槍手","安陽正官庄赤紅火箭","水原KT爆音","韓國石油公社","原州東浮新世代"]
NHL_team = ["紐澤西魔鬼","紐約島人","紐約遊騎兵","費城飛人","匹茲堡企鵝","卡羅萊納颶風","華盛頓首都","波士頓棕熊","水牛城軍刀","多倫多楓葉","佛羅里達美洲豹","坦帕灣閃電","溫尼伯噴射機","芝加哥黑鷹","納許維爾掠奪者","聖路易藍調","科羅拉多雪崩","達拉斯星辰","卡加利火焰","愛德蒙頓油人","溫哥華加人","安納罕鴨","洛杉磯國王","聖荷西鯊魚","維加斯黃金騎士","西雅圖海怪"]
team_pattern = "|".join(NBA_team + NHL_team + MLB_team + NPB_team + Korea_team)

HELP_MSG = """🏆 勝負密碼 使用說明

📌 指令格式：
直接輸入目標即可

📌 範例：
NBA
MLB
足球
日本職棒

📌 支援目標：
NBA / MLB / 足球 / 日本職棒
NHL冰球 / 美式足球 / 歐洲職籃
韓國職籃 / 中國職籃 / 日本職籃

📌 說明：
本系統自動抓取本月主推榜前30名高手
對明日比賽的預測方向進行統計

⚠️ 爬取需要 3~10 分鐘，請耐心等候"""

class Leaderboard:
    def __init__(self, alliance, page):
        self.alliance = alliance
        self.page = page
        self.web_url = "https://www.playsport.cc/"
        self.user_url = self.web_url + "visit_member.php?visit="
        self.header = {"User-Agent": user_agent.random, "Referer": random.choice(back_links)}
        self.html_content = BeautifulSoup(self.crawl_content, "html.parser")

    @cached_property
    def crawl_content(self):
        url = self.web_url + f"billboard/mainPrediction?during=thismonth&allianceid={self.alliance}&page={self.page}"
        r = requests.get(url=url, headers=self.header, timeout=15)
        return r.text

    @property
    def board_json(self):
        for script in self.html_content.find_all("script"):
            if script.string and "vueData" in script.string:
                result = re.search(r"var vueData = (\{.*\});", script.string)
                if result:
                    return json.loads(result[1])
        raise ValueError("找不到資料")

    @property
    def dataframe(self):
        taiwan = pd.DataFrame(self.board_json["rankers"].get("1", []))
        global_ = pd.DataFrame(self.board_json["rankers"].get("2", []))
        df = pd.concat([global_, taiwan], ignore_index=True)
        df.replace({"mode": {1: "運彩盤賽事", 2: "國際盤賽事", "1": "運彩盤賽事", "2": "國際盤賽事"}}, inplace=True)
        df["linkUrl"] = self.user_url + df["userid"] + f"&allianceid={self.alliance}&gameday=tomorrow"
        return df

class RankUser:
    def __init__(self, user_data):
        self.user_data = user_data
        self.header = {"User-Agent": user_agent.random, "Referer": random.choice(back_links)}
        self.html_content = BeautifulSoup(self.crawl_content, "html.parser")

    @cached_property
    def crawl_content(self):
        r = requests.get(url=self.user_data.linkUrl, headers=self.header, timeout=15)
        return r.text

    @property
    def prediction(self):
        def clean_table(table):
            mode = table.columns[0]
            table = table.iloc[:, 1:].copy()
            table.columns = ["game", "prediction", "result"]
            table["userid"] = self.user_data["userid"]
            table["nickname"] = self.user_data["nickname"]
            table["mode"] = mode
            return table[table["game"] != "無預測"][["userid","nickname","mode","game","prediction","result"]]

        def is_main_push(pred_list):
            return ["主推" in "".join(str(i) for i in pred_list[g]) for g in range(len(pred_list))]

        tablebox = pd.DataFrame()
        try:
            tables = pd.read_html(StringIO(self.crawl_content))
            pred_list = self.html_content.find_all("td", class_="managerpredictcon")
            uni, bank = pd.DataFrame(), pd.DataFrame()
            for t in tables:
                if t.columns[0] == "國際盤賽事":
                    uni = clean_table(t)
                elif t.columns[0] == "運彩盤賽事":
                    bank = clean_table(t)
            if uni.shape[0] != 0 or bank.shape[0] != 0:
                tablebox = pd.concat([uni, bank], ignore_index=True)
                tablebox["main_push"] = is_main_push(pred_list)
        except:
            pass
        return tablebox

def has_score(s):
    return bool(re.search(r'^\d+\s', str(s)))

def run_crawler(target, user_id):
    try:
        alliance = alliance_dict.get(target)
        if not alliance:
            push_message(user_id, f"❌ 不支援 {target}")
            return

        leaderboard = pd.DataFrame()
        for page in range(2):
            try:
                r = Leaderboard(alliance, page)
                temp = r.dataframe
                temp = temp[temp["mode"] == "國際盤賽事"]
                leaderboard = pd.concat([leaderboard, temp], ignore_index=True)
            except:
                pass

        if leaderboard.empty:
            push_message(user_id, "❌ 無法取得排行榜，請稍後再試")
            return

        leaderboard = leaderboard.head(30)

        all_pred = pd.DataFrame()
        collected = 0
        for i in range(len(leaderboard)):
            try:
                user = RankUser(leaderboard.iloc[i])
                pred = user.prediction
                if pred.shape[0] > 0:
                    all_pred = pd.concat([all_pred, pred], ignore_index=True)
                    collected += 1
            except:
                pass
            time.sleep(random.uniform(1, 3))

        if all_pred.empty:
            push_message(user_id, "❌ 沒有收集到預測資料")
            return

        merge = pd.merge(
            leaderboard[["userid","wingame","losegame","winpercentage","mode"]],
            all_pred, on="userid"
        )
        merge = merge[merge["mode_x"] == merge["mode_y"]]
        mp = merge[merge["main_push"]].copy()
        mp = mp[~mp["game"].apply(has_score)].copy()

        if mp.empty:
            push_message(user_id, f"⚠️ {target} 目前沒有明日比賽的預測\n高手們可能還沒下注，請晚點再試")
            return

        def extract_game(s):
            m = re.search(rf"({team_pattern})\s*({team_pattern})", str(s))
            return f"{m.group(1)} vs {m.group(2)}" if m else str(s)

        def clean_pred(s):
            return re.sub(r"\d+|[贏輸%.]", "", str(s)).strip()

        mp["game2"] = mp["game"].apply(extract_game)
        mp["pred2"] = mp["prediction"].apply(clean_pred)

        top = (mp.groupby(["game2","pred2"]).size()
               .reset_index(name="count")
               .sort_values("count", ascending=False)
               .head(10))

 lines = [f"🏆 {target} 明日賽事主推統計\n本月榜前30名・共{collected}人預測\n"]
for _, row in top.iterrows():
    try:
        count = int(row['count'])
        if count >= 7:
            confidence = "💪 強"
        elif count >= 4:
            confidence = "👍 中"
        else:
            confidence = "⚠️ 弱"
        lines.append(f"📊 {row['game2']}")
        lines.append(f"   → {row['pred2']}　{count}人推")
        lines.append(f"   信心指數：{confidence}（{count}/30人）")
        lines.append("")
    except Exception:
        pass

lines.append("─────────────────")
lines.append("💬 加入運彩討論群")
lines.append("LINE 搜尋 st130330")

        push_message(user_id, "\n".join(lines))

    except Exception as e:
        push_message(user_id, f"❌ 發生錯誤：{str(e)}")

def push_message(user_id, text):
    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.push_message(
                PushMessageRequest(
                    to=user_id,
                    messages=[TextMessage(text=text)]
                )
            )
    except Exception as e:
        print(f"Push error: {e}")

@app.route("/")
def index():
    return "OK", 200

@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return "OK", 200

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    text = event.message.text.strip()
    user_id = event.source.user_id

    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)

        if text in ["help", "Help", "說明", "使用說明", "?"]:
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[TextMessage(text="⏳ 載入中...")]
                )
            )
            push_message(user_id, HELP_MSG)
            return

        elif text in alliance_dict:
            t = threading.Thread(
                target=run_crawler,
                args=(text, user_id),
                daemon=True
            )
            t.start()
            reply = f"⏳ 開始統計 {text} 明日賽事主推\n本月榜前30名高手預測中\n約需 5~10 分鐘，完成後自動回傳..."

        else:
            reply = "❓ 格式錯誤\n請輸入「說明」查看使用方式"

        line_bot_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text=reply)]
            )
        )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
