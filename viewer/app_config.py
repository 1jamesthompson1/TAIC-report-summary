import os
from datetime import timedelta

AUTHORITY = os.getenv("AUTHORITY")

# Application (client) ID of app registration
CLIENT_ID = os.getenv("CLIENT_ID")
# Application's generated client secret: never check this into source control!
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

REDIRECT_PATH = "/getAToken"  # Used for forming an absolute URL to your redirect URI.
ROOT_URI = os.getenv("ROOT_URI")
REDIRECT_URI = ROOT_URI + REDIRECT_PATH

ENDPOINT = "https://graph.microsoft.com/v1.0/me"
SCOPE = ["User.Read"]

# Tells the Flask-sessixon extension to store sessions in the filesystem
SESSION_TYPE = "filesystem"
PERMANENT_SESSION_LIFETIME = timedelta(days=3)
SESSION_FILE_THRESHOLD = 50
