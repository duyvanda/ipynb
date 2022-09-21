import base64
import os.path
import pickle
import pandas as pd
import tqdm

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build as google_service_build
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import mimetypes
from apiclient import errors

from collector.core.abstract_utils import AbstractUtils

# If modifying these scopes, delete the file token.pickle.
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly",
          "https://www.googleapis.com/auth/gmail.send"]


class GMailUtils(AbstractUtils):
    def __init__(self, config: dict):
        super().__init__(config)
        self.cert_path = config.get("cert_path")
        self.creds_obj = None
        self.refresh_token()
        self.gmail_service = google_service_build(
            "gmail", "v1", credentials=self.creds_obj
        )

    def refresh_token(self):
        token_file = os.path.join(self.cert_path, "token.pickle")
        credentials_file = os.path.join(self.cert_path, "credentials.json")
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(token_file):
            with open(token_file, "rb") as token:
                self.creds_obj = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not self.creds_obj or not self.creds_obj.valid:
            if (
                self.creds_obj
                and self.creds_obj.expired
                and self.creds_obj.refresh_token
            ):
                self.creds_obj.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_file, SCOPES
                )
                self.creds_obj = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(token_file, "wb") as token:
                pickle.dump(self.creds_obj, token)

    def execute(self, **kwargs):
        pass

    def create_draft(self, user_id, message_body):
        """Create and insert a draft email. Print the returned draft's message and id.

        Args:
          self.service: Authorized Gmail API service instance.
          user_id: User's email address. The special value "me"
          can be used to indicate the authenticated user.
          message_body: The body of the email message, including headers.

        Returns:
          Draft object, including draft id and message meta data.
        """
        try:
            message = {'message': message_body}
            draft = self.gmail_service.users().drafts().create(userId=user_id, body=message).execute()

            print('Draft id: %s\nDraft message: %s' % (draft['id'], draft['message']))

            return draft
        except errors.HttpError as error:
            print('An error occurred: %s' % error)
            return None

    def read_inbox(self, archive_path):
        # Call the Gmail API
        results = (
            self.gmail_service.users().messages().list(userId="me", labelIds=["INBOX"], maxResults=1000).execute()
        )
        messages = results.get("messages", [])
        lst_rs = list()
        if not messages:
            print("No messages found.")
        else:
            print("Message snippets:")
            num_records = len(messages)
            with tqdm.tqdm(total=num_records) as pbar:
                for message in messages:
                    msg = (
                        self.gmail_service.users()
                            .messages()
                            .get(userId="me", id=message["id"])
                            .execute()
                    )
                    msg_id = message["id"]
                    mime_msg = None
                    msg_headers = pd.DataFrame(msg["payload"]["headers"])
                    msg_subject = msg_headers.loc[
                        msg_headers["name"] == "Subject", "value"
                    ].iloc[0]
                    msg_snippet = msg["snippet"]
                    parts = msg["payload"].get("parts", list())
                    lst_in_paths = list()
                    for part in parts:
                        file_name = part["filename"]
                        if file_name:
                            if "data" in part["body"]:
                                data = part["body"]["data"]
                            else:
                                att_id = part["body"]["attachmentId"]
                                att = (
                                    self.gmail_service.users()
                                        .messages()
                                        .attachments()
                                        .get(userId="me", messageId=msg_id, id=att_id)
                                        .execute()
                                )
                                data = att["data"]
                            file_data = base64.urlsafe_b64decode(data.encode("UTF-8"))
                            in_path = os.path.join(archive_path, file_name)
                            with open(in_path, "wb") as f:
                                f.write(file_data)
                                f.close()
                            lst_in_paths.append(in_path)

                        else:
                            mime_type = part["mimeType"]
                            # print(part['mimeType'])
                            if mime_type == "text/plain":
                                mime_msg = base64.urlsafe_b64decode(
                                    part["body"]["data"].encode("ASCII")
                                ).decode("utf-8")

                            if mime_type == "multipart/alternative":
                                for p in part["parts"]:
                                    m = p["mimeType"]
                                    if m == "text/plain":
                                        mime_msg = base64.urlsafe_b64decode(
                                            p["body"]["data"].encode("ASCII")
                                        ).decode("utf-8")

                    if mime_msg is None:
                        mime_msg = ""
                    lst_rs.append(
                        {
                            "id": msg_id,
                            "subject": msg_subject,
                            "snippet": msg_snippet,
                            "in_paths": lst_in_paths,
                            "message": mime_msg,
                        }
                    )
                    pbar.update(1)

                df = pd.DataFrame(lst_rs)
                return df

    def send_message(self, user_id, message):
        """Send an email message.

        Args:
          self.service: Authorized Gmail API service instance.
          user_id: User's email address. The special value "me"
          can be used to indicate the authenticated user.
          message: Message to be sent.

        Returns:
          Sent Message.
        """
        try:
            message = (self.gmail_service.users().messages().send(userId=user_id, body=message)
                       .execute())
            print('Message Id: %s' % message['id'])
            return message
        except errors.HttpError as error:
            print('An error occurred: %s' % error)

    @staticmethod
    def create_message(sender, to, subject, message_text, subtype="plain", cc=None):
        """Create a message for an email.

        Args:
          sender: Email address of the sender.
          to: Email address of the receiver.
          cc: Email address of the cc
          subject: The subject of the email message.
          message_text: The text of the email message.
          subtype: plain/html

        Returns:
          An object containing a base64url encoded email object.
        """
        message = MIMEText(message_text, subtype)
        message['to'] = to
        message['from'] = sender
        message['subject'] = subject
        if cc is not None:
            message['cc'] = cc
        return {'raw': base64.urlsafe_b64encode(message.as_string().encode('utf-8')).decode('utf-8')}

    @staticmethod
    def create_message_with_attachment(
            sender, to, subject, message_text, lst_file):
        """Create a message for an email.

        Args:
          sender: Email address of the sender.
          to: Email address of the receiver.
          subject: The subject of the email message.
          message_text: The text of the email message.
          file: The path to the file to be attached.

        Returns:
          An object containing a base64url encoded email object.
        """
        message = MIMEMultipart()
        message['to'] = to
        message['from'] = sender
        message['subject'] = subject

        msg = MIMEText(message_text)
        message.attach(msg)

        for file in lst_file:
            content_type, encoding = mimetypes.guess_type(file)

            if content_type is None or encoding is not None:
                content_type = 'application/octet-stream'
            main_type, sub_type = content_type.split('/', 1)
            if main_type == 'text':
                fp = open(file, 'rb')
                msg = MIMEText(fp.read().decode('utf-8'), _subtype=sub_type)
                fp.close()
            elif main_type == 'image':
                fp = open(file, 'rb')
                msg = MIMEImage(fp.read(), _subtype=sub_type)
                fp.close()
            elif main_type == 'audio':
                fp = open(file, 'rb')
                msg = MIMEAudio(fp.read(), _subtype=sub_type)
                fp.close()
            else:
                fp = open(file, 'rb')
                msg = MIMEBase(main_type, sub_type)
                msg.set_payload(fp.read())
                fp.close()
            filename = os.path.basename(file)
            msg.add_header('Content-Disposition', 'attachment', filename=filename)
            message.attach(msg)

        return {'raw': base64.urlsafe_b64encode(message.as_string().encode('utf-8')).decode('utf-8')}


if __name__ == "__main__":
    conf = {
        "cert_path": "/Users/thucpk/IdeaProjects/data-warehouse/data-collector/config/gsuide/gmail",
    }
    gmail_utils = GMailUtils(conf)
    gmail_utils.refresh_token()
#     text_message = """
# Dear Linh Nhi,
#
# As the process for setting targets, we would like to send you the Daily Target Setting link to update September Daily Target.
# Please help us to update the file below before 10am 29/08/2020. Link: https://docs.google.com/spreadsheets/d/1rTrDa0EXMtQe6WMSMQ_aqKxhbi8lh9pqWOkj_XQxBQA/edit?ts=5f33d15c#gid=0
# Thank you a lot for your support and feel free to let us know if any concern
#
# Best Regards,
# BI Team
# """
    # new_message = gmail_utils.create_message(
    #     sender="BI-team@onpoint.vn", to="linhnhi.ta@onpoint.vn",
    #     subject="Daily Target 09/2020",
    #     message_text=text_message, cc="BI-team@onpoint.vn,commercial@onpoint.vn")
    # gmail_utils.send_message("me", new_message)

    new_message = gmail_utils.create_message_with_attachment(
        sender="kienthuc.phan@onpoint.vn", to="kienthuc.phan@onpoint.vn",
        subject="Test attach", message_text="body test", lst_file=[
            "/Users/thucpk/IdeaProjects/data-warehouse/docs/apache_setup.md",
            "/Users/thucpk/IdeaProjects/data-warehouse/docs/jupyterhub_setup.md"]
    )
    gmail_utils.send_message("me", new_message)
