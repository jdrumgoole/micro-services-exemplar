import os

class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'F9DP7SBS0RE245214LNC'
