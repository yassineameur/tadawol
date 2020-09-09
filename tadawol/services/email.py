from datetime import datetime
import smtplib
from email.mime.text import MIMEText

from tadawol.config import EmailConfig


def send_email(text_to_send):

    msg = MIMEText(text_to_send, "html")
    email_config = EmailConfig()
    msg['Subject'] = f"{datetime.today().date()} tickers"

    email_address = email_config.destination
    msg['From'] = email_address
    msg['To'] = email_address

    server = smtplib.SMTP(email_config.host, email_config.port)
    server.ehlo()
    server.starttls()
    server.login(email_config.username, email_config.password)
    server.sendmail(email_address, [email_address], msg.as_string())
    server.quit()
