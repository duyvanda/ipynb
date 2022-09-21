from dateutil import tz
import datetime
import calendar

from collector.import_gdrive.abs_import_gdrive import AbstractImportBase
from collector.libs.logger import get_logger
from collector.gsuide_libs.gmail_utils import GMailUtils
from collector.libs.render_string import render_template


logger = get_logger(__name__)
local_tz = tz.tzlocal()


message_template = """
Dear {{ receiver_name }},

As the process for setting targets, we would like to send you the Daily Target Setting link to update {{ month_name }} Daily Target.
Please help us to update the file below before {{ due_date }}. Link: {{ link }}
Thank you a lot for your support and feel free to let us know if any concern

Best Regards,
BI Team
"""


class TaskRemind(AbstractImportBase):

    def __init__(self, config):
        super().__init__(config)
        gmail_cert_path = self.get_param_config(["gmail_cert"])
        gmail_config = {
            "cert_path": gmail_cert_path,
        }
        print(gmail_config)
        self.gmail_utils = GMailUtils(gmail_config)
        self.gmail_utils.refresh_token()
        self.gmail_info = self.get_param_config(["gmail_info"])
        self.link = self.get_param_config(['link'])

    @AbstractImportBase.wrapper_simple_log
    def execute(self, *args):
        subject = self.execution_date.strftime("Daily Target %m/%Y")
        next_month = (self.execution_date + datetime.timedelta(10)).month
        due_date = (self.execution_date + datetime.timedelta(2)).strftime("%H:%M %Y-%m-%d")
        receiver_name = self.gmail_info["receiver_name"]
        sender = self.gmail_info["sender"]
        receiver = self.gmail_info['receiver']
        cc = self.gmail_info["cc"]

        message_text = render_template(message_template, {
            "receiver_name": receiver_name,
            "month_name": calendar.month_name[next_month],
            "due_date": due_date,
            "link": self.link
        })
        new_message = self.gmail_utils.create_message(
            sender=sender, to=receiver,
            subject=subject,
            message_text=message_text, cc=cc
        )
        self.gmail_utils.send_message("me", new_message)


if __name__ == "__main__":
    import json

    conf = {
        "app": {
            "process_type": "import_gdrive",
            "process_group": "daily_target",
            "process_name": "linhnhi",
            "process_action": "remind",
            "execution_date": datetime.datetime.now(),
            "from_date": datetime.datetime.now() - datetime.timedelta(10),
            "to_date": datetime.datetime.now(),
            "params": {
                "telegram": json.load(
                    open(
                        "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/telegram.json"
                    )
                ),
                "telegram_users": json.load(
                    open(
                        "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/telegram_users.json"
                    )
                ),
                "gdrive": json.load(
                    open(
                        "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/default/gdrive_local.json"
                    )
                ),
                "gmail_cert": "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/gsuide/gmail",
                "gmail_info": {
                    "sender": "kienthuc.phan@onpoint.vn",
                    "receiver_name": "Kien Thuc",
                    "receiver": "kienthuc.phan@onpoint.vn",
                    "cc": "kienthuc.phan@onpoint.vn"
                },
                "link": "https://docs.google.com/spreadsheets/d/1rTrDa0EXMtQe6WMSMQ_aqKxhbi8lh9pqWOkj_"
                        "XQxBQA/edit?ts=5f33d15c#gid=0"
            },
        }
    }
    fetch_data = TaskRemind(config=conf)
    fetch_data.execute()
