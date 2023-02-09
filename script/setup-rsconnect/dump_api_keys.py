import json
import sys

from pins.rsconnect.api import _HackyConnect

OUT_FILE = sys.argv[1]


def get_api_key(user, password, email):
    rsc = _HackyConnect("http://localhost:3939")

    return rsc.create_first_admin(user, password, email).api_key


api_keys = {
    "admin": get_api_key("admin", "zzzzz000", "admin@example.com"),
    "susan": get_api_key("susan", "zzzzz000", "susan@example.com"),
    "derek": get_api_key("derek", "zzzzz000", "derek@example.com"),
}

json.dump(api_keys, open(OUT_FILE, "w"))
