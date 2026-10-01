import json

_SESSION_KEY = "_datalayer_agent_events"
_MAX_QUEUED_EVENTS = 100


def _validated_event(event, parameters=None):
    if not isinstance(event, str) or not event.strip() or len(event) > 100:
        raise ValueError("Event names must contain 1 to 100 characters")
    if event.startswith("gtm."):
        raise ValueError("The gtm.* namespace is reserved")
    values = dict(parameters or {})
    if "event" in values:
        raise ValueError("Event parameters cannot override the event name")
    json.dumps(values, allow_nan=False)
    return {"event": event, **values}


def serialize_data_layer_events(events):
    value = json.dumps(events, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    return (value.replace("&", "\u0026").replace("<", "\u003c")
            .replace(">", "\u003e").replace("\u2028", "\\u2028")
            .replace("\u2029", "\\u2029"))


def queue_data_layer_event(request, event, parameters=None):
    events = list(request.session.get(_SESSION_KEY, []))
    if len(events) >= _MAX_QUEUED_EVENTS:
        raise ValueError("Too many queued dataLayer events")
    events.append(_validated_event(event, parameters))
    request.session[_SESSION_KEY] = events
    request.session.modified = True


def consume_data_layer_events(request):
    events = request.session.pop(_SESSION_KEY, [])
    request.session.modified = True
    return serialize_data_layer_events(events)
