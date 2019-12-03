from smtplib import SMTP_SSL
from os import getenv
from email.mime.text import MIMEText


class Mailer:
    def __init__(self):
        self.user = getenv("GMAIL_USER")
        self.password = getenv("GMAIL_PWD")
        self.slack_email = getenv("SLACK_EMAIL")
        self.server = SMTP_SSL("smtp.gmail.com", 465)
        self.from_address = "KIPP Bay Area Job Notification"
        self.to_address = "databot"

    def _subject_line(self):
        subject_type = "Success" if self.success else "Error"
        return f"Deanslist_Connector - {subject_type}"

    def _body_text(self):
        if self.success:
            return f"The Deanslist Connector job ran successfully.\n{self.logs}"
        else:
            return f"The Deanslist Connector job encountered an error:\n{self.logs}\n{self.error_message}"

    def _message(self):
        msg = MIMEText(self._body_text())
        msg["Subject"] = self._subject_line()
        msg["From"] = self.from_address
        msg["To"] = self.to_address
        return msg.as_string()

    def _read_logs(self, filename):
        with open(filename) as f:
            return f.read()

    def notify(self, success=True, error_message=None):
        self.success = success
        self.logs = self._read_logs("app.log")
        self.error_message = error_message
        with self.server as s:
            s.login(self.user, self.password)
            msg = self._message()
            s.sendmail(self.user, self.slack_email, msg)
