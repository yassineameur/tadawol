from pydantic import BaseSettings


class EmailConfig(BaseSettings):

    api: str
    source: str
    destination: str

    class Config:
        allow_mutation = False
        env_prefix = "email_"


class BrokerConfig(BaseSettings):

    url: str

    class Config:
        allow_mutation = False
        env_prefix = "amqp_"
