import telegram
from telegram.utils.request import Request


class TelegramUtils(object):
    def __init__(self, config: dict):
        tkn = config["bot_token"]
        https_proxy = config.get("https_proxy")
        if https_proxy:
            pp = Request(proxy_url=https_proxy)
            self.bot = telegram.Bot(token=tkn, request=pp)
        else:
            self.bot = telegram.Bot(token=tkn)

    def send_message(self, user_id, msg, parse_mode="Markdown"):
        self.bot.send_message(user_id, msg, parse_mode=parse_mode)

    def send_message_with_attachment(self, user_id, msg, file):
        self.bot.send_message(user_id, msg)
        self.bot.send_document(user_id, msg, file)


# if __name__ == '__main__':
#     import json
#     telegram_conf = json.load(open("/Users/thucpk/IdeaProjects/onpoint/onboard/config/default/telegram.json"))
#     telegram_users = json.load(open("/Users/thucpk/IdeaProjects/onpoint/onboard/config/default/telegram_users.json"))
#     telegram_bot = TelegramUtils(telegram_conf)
#     telegram_bot.send_message(telegram_users['thucpk'], "hello")
