import json
import os
import sys

from pins.rsconnect.api import LoginConnectApi, RsConnectApi

OUT_FILE = sys.argv[1]

extra_users = [
    {"username": "susan", "password": "susansusan"},
    {"username": "derek", "password": "derekderek"},
]

# Assumes CONNECT_SERVER and CONNECT_API_KEY are set in the environment
admin_client = RsConnectApi()

# Rename admin user to "admin" ¯\_(ツ)_/¯
guid = admin_client.get_user()["guid"]
admin_client.query_v1(f"users/{guid}", "PUT", json={"username": "admin"})

api_keys = {"admin": os.getenv("CONNECT_API_KEY")}

for user in extra_users:
    # Create user
    admin_client.create_user(
        username=user["username"],
        password=user["password"],
        __confirmed=True,
    )
    # Log in as them and generate an API key, and add to dict
    api_keys[user["username"]] = LoginConnectApi(
        user["username"], user["password"]
    ).create_api_key()

json.dump(api_keys, open(OUT_FILE, "w"))
