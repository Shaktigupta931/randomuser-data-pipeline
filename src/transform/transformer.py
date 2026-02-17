import pandas as pd
from datetime import datetime

def transform_data(raw_json):
    users = []
    names = []
    locations = []
    logins = []
    dobs = []
    ids = []
    pictures = []

    for record in raw_json["results"]:

        # Use login.uuid as unique user_id (IMPORTANT)
        user_id = record["login"]["uuid"]

        # USERS TABLE
        users.append({
            "user_id": user_id,
            "gender": record["gender"],
            "email": record["email"],
            "phone": record["phone"],
            "cell": record["cell"],
            "nat": record["nat"],
            "registered_date": record["registered"]["date"],
            "created_at": datetime.utcnow()
        })

        # NAMES TABLE
        names.append({
            "user_id": user_id,
            "title": record["name"]["title"],
            "first": record["name"]["first"],
            "last": record["name"]["last"]
        })

        # LOCATION TABLE
        locations.append({
            "user_id": user_id,
            "street_number": record["location"]["street"]["number"],
            "street_name": record["location"]["street"]["name"],
            "city": record["location"]["city"],
            "state": record["location"]["state"],
            "country": record["location"]["country"],
            "postcode": str(record["location"]["postcode"]),
            "latitude": record["location"]["coordinates"]["latitude"],
            "longitude": record["location"]["coordinates"]["longitude"],
            "timezone_offset": record["location"]["timezone"]["offset"],
            "timezone_description": record["location"]["timezone"]["description"]
        })

        # LOGIN TABLE
        logins.append({
            "user_id": user_id,
            "username": record["login"]["username"],
            "password": record["login"]["password"],
            "salt": record["login"]["salt"],
            "md5": record["login"]["md5"],
            "sha1": record["login"]["sha1"],
            "sha256": record["login"]["sha256"]
        })

        # DOB TABLE
        dobs.append({
            "user_id": user_id,
            "dob": record["dob"]["date"],
            "age": record["dob"]["age"]
        })

        # ID TABLE
        ids.append({
            "user_id": user_id,
            "id_name": record["id"]["name"],
            "id_value": record["id"]["value"]
        })

        # PICTURE TABLE
        pictures.append({
            "user_id": user_id,
            "large": record["picture"]["large"],
            "medium": record["picture"]["medium"],
            "thumbnail": record["picture"]["thumbnail"]
        })

    return {
        "users": pd.DataFrame(users),
        "names": pd.DataFrame(names),
        "locations": pd.DataFrame(locations),
        "logins": pd.DataFrame(logins),
        "dobs": pd.DataFrame(dobs),
        "ids": pd.DataFrame(ids),
        "pictures": pd.DataFrame(pictures),
    }
