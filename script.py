# {
#   "kind": "admin#reports#activity",
#   "id": {
#     "time": "2025-09-20T06:57:09.025Z",
#     "uniqueQualifier": "4366088746345401094",
#     "applicationName": "token",
#     "customerId": "C02f6wppb"
#   },
#   "etag": "\"FQEYJqXpeOV6_3YH8wsi-hSMuqN7BUDbJo9YY4QJAu4/k21MnaD_b3CpfXKGuphv4JcxpwQ\"",
#   "actor": {
#     "email": "nancy.admin@hyenacapital.net",
#     "profileId": "100230688039070881323"
#   },
#   "events": [
#     {
#       "type": "auth",
#       "name": "activity",
#       "parameters": [
#         {
#           "name": "api_name",
#           "value": "gmail"
#         },
#         {
#           "name": "method_name",
#           "value": "gmail.users.history.list"
#         },
#         {
#           "name": "client_id",
#           "value": "109303849349009433243"
#         },
#         {
#           "name": "num_response_bytes",
#           "intValue": "5"
#         },
#         {
#           "name": "product_bucket",
#           "value": "GMAIL"
#         },
#         {
#           "name": "app_name",
#           "value": "109303849349009433243"
#         },
#         {
#           "name": "client_type",
#           "value": "WEB"
#         }
#       ]
#     }
#   ]
# }

import json
from pathlib import Path
with open(Path("2025-09-18T06:42:11.459466+00:00_48_hour_events.json")) as f:
    user_activity_count: dict[str, int] = {}
    bytes_responded: dict[str, int] = {}
    for line in f:
        activity = json.loads(line)
        id = activity.get("actor", {}).get("profileId")
        # method_name = activity.get("events", {}).get("parameters", [{}])[0].get("method_name")
        method_name = next((param.get("value") for param in activity.get("events", [{}])[0].get("parameters", [{}]) if param.get("name") == "method_name"), None)
        bytes = next((int(param.get("intValue")) for param in activity.get("events", [{}])[0].get("parameters", [{}]) if param.get("name") == "num_response_bytes"), None)
        if bytes is not None and method_name is not None:
            bytes_responded[method_name] = bytes_responded.get(method_name, 0) + bytes
        if id is not None:
            user_activity_count[id] = user_activity_count.get(id, 0) + 1
        
    # for id, count in user_activity_count.items():
        # print(f"User {id} has {count} activities")
    for method, count in bytes_responded.items():
        print(f"Method {method} has {count} bytes responded")
    print(f"Found {len(user_activity_count)} unique users")
    print(f"Most active user: {max(user_activity_count, key=lambda x: user_activity_count[x])} with {max(user_activity_count.values())} activities")
    print(f"Method with most bytes responded: {max(bytes_responded, key=lambda x: bytes_responded[x])} with {max(bytes_responded.values())} bytes")
