import smtplib
from email.message import EmailMessage
from vars import vars

def sendmail():
    #Email connection
    EMAIL_ADDRESS = vars['name']
    EMAIL_INFORMATION = vars['information']
    msg = EmailMessage()
    # FROM EMAIL
    msg['From'] = EMAIL_ADDRESS
    # TO EMAIL
    msg['To'] = ['trangnn@phanam.com.vn', 'duyvq@merapgroup.com', 'thaodhh@merapgroup.com']
    # BODY
    msg.set_content('BI TEAM - Report has been generated')
    msg['Subject'] = 'BI BOT - Data Driven Reports 2022-09-22'
    with smtplib.SMTP_SSL('mail.merapgroup.com', 465) as smtp:
        smtp.login(EMAIL_ADDRESS, EMAIL_INFORMATION)
        smtp.send_message(msg)
        print('Message has been sent')
