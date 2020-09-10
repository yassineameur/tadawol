import sendgrid
from sendgrid.helpers.mail import *

from tadawol.config import EmailConfig


def send_email(text_to_send, subject):
    email_config = EmailConfig()
    sg = sendgrid.SendGridAPIClient(api_key=email_config.api)
    from_email = Email(email_config.source)
    to_email = To(email_config.destination)
    content = Content("text/html", text_to_send)
    mail = Mail(from_email, to_email, subject, content)
    response = sg.client.mail.send.post(request_body=mail.get())
    print(response.status_code)
