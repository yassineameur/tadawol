from datetime import datetime
import smtplib
from email.mime.text import MIMEText


def send_email(text_to_send):

    msg = MIMEText(text_to_send, "html")
    msg['Subject'] = f"{datetime.today().date()} tickers"

    email_address = "yassine.ameur2013@gmail.com"
    msg['From'] = email_address
    msg['To'] = email_address

    smtp_port = 587
    smtp_host = "smtp.gmail.com"
    login = "yassine.ameur2013@gmail.com"
    password = "yassineetmariem"

    server = smtplib.SMTP(smtp_host, smtp_port)
    server.ehlo()
    server.starttls()
    server.login(login, password)
    server.sendmail(email_address, [email_address], msg.as_string())
    server.quit()
