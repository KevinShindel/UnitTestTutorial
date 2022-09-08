

def tweet(api, msg: str):
    if len(msg) > 40:
        msg = msg.strip('.!,?')
    status = api.PostUpdate(msg)
    return status

